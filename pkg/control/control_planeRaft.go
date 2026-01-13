package control

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	nadzorna_ravnina "github.com/djagodic/razpravljalnica2/pkg/api/nadzornaRavnina"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// ControlPlaneServer implements api.ControlPlane
type ControlPlaneServer struct {
	nadzorna_ravnina.UnimplementedControlPlaneServer

	address     string // gRPC
	raftAddress string // Raft transport

	// Hooks used when the control plane is wrapped by RaftControlPlane.
	leaderCheck       func() bool
	applyDeregisterFn func(nodeID string) error
	applySetNodesFn   func(nodes []*NodeInfo) error

	mu sync.RWMutex
	// subToChanges is accessed from Register/Deregister (which hold mu) AND from
	// SubscribeToChanges stream goroutines. A Go RWMutex is not re-entrant, so
	// calling sendChanges() (which used to RLock mu) while holding mu.Lock()
	// deadlocked the leader on the 2nd server registration.
	//
	// We therefore protect subscriptions with a dedicated mutex.
	subsMu       sync.RWMutex
	nodes        []*NodeInfo // ordered chain: head -> ... -> tail
	nodeMap      map[string]*NodeInfo
	interval     time.Duration
	subToChanges map[string]chan *nadzorna_ravnina.Changes //subscriberji na spremembe -> ch
}

type NodeInfo struct {
	NodeID  string
	Address string
	Alive   bool
	LastHB  time.Time
}

// NewControlPlaneServer creates a new control plane instance
func NewControlPlaneServer(iPnaslov string, raftnaslov string) *ControlPlaneServer {
	return &ControlPlaneServer{
		address:      iPnaslov,
		raftAddress:  raftnaslov,
		nodes:        []*NodeInfo{},
		nodeMap:      make(map[string]*NodeInfo),
		interval:     5 * time.Second, // heartbeat interval
		subToChanges: make(map[string]chan *nadzorna_ravnina.Changes),
	}
}

// RegisterNode adds a new node to the chain (called by node at startup)
func (c *ControlPlaneServer) registerNodeInternal(ctx context.Context, req *nadzorna_ravnina.RegisterNodeRequest) (*nadzorna_ravnina.RegisterNodeResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for idx, nodeInfo := range c.nodes {
		//node je ze registriran
		//ne sprejmemo nodeov z istim imenom v verigo
		if nodeInfo.NodeID == req.NodeId {
			log.Printf("node %s already registered, at idx %d", req.NodeId, idx)
			//doloci head, tail
			isHead := false
			isTail := false
			if idx == 0 {
				isHead = true
			}
			if idx == len(c.nodes)-1 {
				isTail = true
			}

			return &nadzorna_ravnina.RegisterNodeResponse{
				Success: false,
				Message: "node already registered",
				IsHead:  isHead,
				IsTail:  isTail,
			}, nil
		}
	}

	//node se ni registriran
	node := &NodeInfo{
		NodeID:  req.NodeId,
		Address: req.Address,
		Alive:   true,
		LastHB:  time.Now(),
	}
	//dodamo nov node na konec in v nodeMap
	c.nodes = append(c.nodes, node)
	c.nodeMap[req.NodeId] = node
	log.Printf("registered node: %s (%s)", req.NodeId, req.Address)

	//doloci head in tail
	isHead := false
	isTail := true
	if len(c.nodes) == 1 { //ce je to edini node je tudi head
		isHead = true
	}

	//preveri ali moras predhodnje node obvestiti
	if len(c.nodes) >= 2 {
		predzadnji := c.nodes[len(c.nodes)-2]
		zadnji := c.nodes[len(c.nodes)-1]
		//posljemu prejsnjemu repu obvestilo o novem repu
		err := c.sendChanges(predzadnji, zadnji, false)
		if err != nil {
			return &nadzorna_ravnina.RegisterNodeResponse{
				Success: false,
				Message: "sprememba ob registraciji neuspesna",
				IsHead:  isHead,
				IsTail:  isTail,
			}, fmt.Errorf("sprememba ob registraciji nodeId: %s ni bila uspesna", node.NodeID)
		}
	}

	return &nadzorna_ravnina.RegisterNodeResponse{
		Success: true,
		Message: "registered node sucessfully",
		IsHead:  isHead,
		IsTail:  isTail,
	}, nil
}

// DeregisterNode removes a node from the chain
func (c *ControlPlaneServer) DeregisterNode(nodeID string) {
	fmt.Printf("DEBUG: deregistriramo node %s\n", nodeID)

	// IMPORTANT:
	// DeregisterNode can be called from multiple goroutines:
	// - gRPC handlers (non-Raft mode)
	// - the monitor loop
	// - the Raft FSM apply goroutine
	// Therefore it MUST be internally synchronized.
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.nodeMap[nodeID]
	//ce server ni v node Map pac ni registriran in vrnes nic
	if !ok {
		return
	}

	//ga izbrisemo iz map
	delete(c.nodeMap, nodeID)

	//ga izbrisemo iz nodes
	for i, n := range c.nodes {
		if n.NodeID == nodeID {
			// If this is the only node in the chain, just remove it.
			// (No predecessor/successor exists, so nothing to notify.)
			if len(c.nodes) == 1 {
				c.nodes = nil
				break
			}

			// Otherwise, notify affected neighbors.
			if i != 0 && i != len(c.nodes)-1 { // middle node
				fmt.Printf("DEBUG: poslal sendchanges v %s in %s\n", c.nodes[i-1].NodeID, c.nodes[i+1].NodeID)
				_ = c.sendChanges(c.nodes[i-1], c.nodes[i+1], false)
			} else if i != 0 && i == len(c.nodes)-1 { // tail
				fmt.Printf("DEBUG: poslal sendchanges v %s in 'nil'\n", c.nodes[i-1].NodeID)
				_ = c.sendChanges(c.nodes[i-1], nil, false)
			} else if i == 0 { // head
				// New head is the next node (index 1)
				_ = c.sendChanges(c.nodes[1], nil, true)
				fmt.Printf("DEBUG: odpovedal head: new head=%s\n", c.nodes[1].NodeID)
			}
			//izbrisemo iz verige
			c.nodes = append(c.nodes[:i], c.nodes[i+1:]...)

			break
		}
	}

	log.Printf("deregistered node: %s", nodeID)
}

// Heartbeat updates last seen timestamp
func (c *ControlPlaneServer) Heartbeat(ctx context.Context, req *nadzorna_ravnina.HeartbeatRequest) (*emptypb.Empty, error) {

	c.mu.Lock()
	defer c.mu.Unlock()

	_, ok := c.nodeMap[req.NodeId]
	if !ok {
		log.Printf("heartbeat from unknown node %s", req.NodeId)
		return &emptypb.Empty{}, nil
	}

	c.heartbeatInternalLocked(req.NodeId, time.Now())

	return &emptypb.Empty{}, nil
}

// heartbeatInternalLocked updates liveness for nodeID. Caller must hold c.mu.
func (c *ControlPlaneServer) heartbeatInternalLocked(nodeID string, hbAt time.Time) {
	n, ok := c.nodeMap[nodeID]
	if !ok {
		// Ignore unknown node heartbeats (node may have been deregistered).
		return
	}
	if hbAt.IsZero() {
		hbAt = time.Now()
	}
	n.LastHB = hbAt
	n.Alive = true
}

// Periodic health check to detect dead nodes
func (c *ControlPlaneServer) monitorNodes() {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for range ticker.C {
		// When running under Raft, ONLY the leader should run failure detection / reconfiguration.
		if c.leaderCheck != nil && !c.leaderCheck() {
			continue
		}

		now := time.Now()
		dead := make([]string, 0)

		c.mu.Lock()
		for _, n := range c.nodes {
			// Defensive: after Raft restore / snapshot, LastHB might be zero.
			if n.LastHB.IsZero() {
				n.LastHB = now
			}
			if !n.Alive {
				continue
			}
			if now.Sub(n.LastHB) > 3*c.interval {
				n.Alive = false
				dead = append(dead, n.NodeID)
				log.Printf("node %s marked DEAD", n.NodeID)
			} else {
				log.Printf("node %s heartbeat successful", n.NodeID)
			}
		}
		c.mu.Unlock()

		// Reconfigure outside the lock to avoid deadlocks and long pauses.
		for _, id := range dead {
			c.reconfigureChain(id)
		}
	}
}

// reconfigureChain is called after node failure.
func (c *ControlPlaneServer) reconfigureChain(deadNodeID string) {
	log.Printf("reconfiguring chain, removing dead node %s", deadNodeID)
	// When running under Raft, make sure the removal is replicated via the leader.
	if c.applyDeregisterFn != nil {
		if err := c.applyDeregisterFn(deadNodeID); err != nil {
			log.Printf("failed to replicate deregister for %s: %v", deadNodeID, err)
			return
		}
		return
	}
	c.DeregisterNode(deadNodeID)
	// Note: in full implementation, would also inform head/tail nodes to update nextNode
}

// GetClusterState returns current head and tail
func (c *ControlPlaneServer) GetClusterState(ctx context.Context, _ *emptypb.Empty) (*nadzorna_ravnina.GetClusterStateResponse, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var head, tail NodeInfo
	if len(c.nodes) > 0 {
		head = *c.nodes[0]
		tail = *c.nodes[len(c.nodes)-1]
	}
	return &nadzorna_ravnina.GetClusterStateResponse{
		Head: &nadzorna_ravnina.NodeInfo{
			NodeId:  head.NodeID,
			Address: head.Address,
		},
		Tail: &nadzorna_ravnina.NodeInfo{
			NodeId:  tail.NodeID,
			Address: tail.Address,
		},
	}, nil
}

func (c *ControlPlaneServer) GetSubscriptionNode(ctx context.Context, req *nadzorna_ravnina.SubscriptionNodeRequest) (*nadzorna_ravnina.SubscriptionNodeResponse, error) {

	c.mu.RLock()
	defer c.mu.RUnlock()

	var alive []*NodeInfo
	for _, n := range c.nodes {
		if n.Alive {
			alive = append(alive, n)
		}
	}

	if len(alive) == 0 {
		return nil, status.Error(
			codes.Unavailable,
			"no alive nodes available",
		)
	}

	sum := req.UserId
	for _, t := range req.TopicId {
		sum += t
	}

	idx := sum % int64(len(alive))
	node := alive[idx]

	token := fmt.Sprintf(
		"%s:%d:%d",
		node.NodeID,
		req.UserId,
		time.Now().Unix(),
	)

	return &nadzorna_ravnina.SubscriptionNodeResponse{
		SubscribeToken: token,
		Node: &nadzorna_ravnina.NodeInfo{
			NodeId:  node.NodeID,
			Address: node.Address,
		},
	}, nil
}

// Start launches monitoring loop
func (c *ControlPlaneServer) Start() {
	go c.monitorNodes()
}

// send changes to subscribers -> poslji trenutnemu obvestilo o novem naslednjem
func (s *ControlPlaneServer) sendChanges(trenuten, naslednji *NodeInfo, isNewHead bool) error {
	var change *nadzorna_ravnina.Changes
	if naslednji == nil {
		if isNewHead {
			change = &nadzorna_ravnina.Changes{NextAdress: "NewHead1234"}
		} else {
			//ce hocemo nastaviti naslednjega na nil bomo poslali prazen string
			change = &nadzorna_ravnina.Changes{NextAdress: ""}
		}

	} else {
		//sporocilo o spremembi
		change = &nadzorna_ravnina.Changes{NextAdress: naslednji.Address}
	}

	// Subscription map is protected by subsMu (NOT by mu) to avoid deadlocks
	// when sendChanges is invoked while holding mu.Lock().
	s.subsMu.RLock()
	ch := s.subToChanges[trenuten.NodeID]
	s.subsMu.RUnlock()

	//pomoje nepotrebno
	if ch == nil {
		return nil
	}

	select {
	case ch <- change:
	default:
		fmt.Printf("DEBUG: subscriber %s is slow", trenuten.NodeID) //drop if subscriber is slow
	}

	return nil
}

// grpc SiuubscribeToChanges
func (s *ControlPlaneServer) SubscribeToChanges(req *nadzorna_ravnina.SubscribeToChangesRequest, stream nadzorna_ravnina.ControlPlane_SubscribeToChangesServer) error {
	ch := make(chan *nadzorna_ravnina.Changes, 10)

	// Store subscriber channel under subsMu (NOT mu).
	s.subsMu.Lock()
	s.subToChanges[req.NodeId] = ch
	s.subsMu.Unlock()

	// ko bo vse skupaj crashnilo zbrišem kanal
	defer func() {
		s.subsMu.Lock()
		delete(s.subToChanges, req.NodeId)
		close(ch)
		s.subsMu.Unlock()
	}()

	//stream new messages
	go func(ch chan *nadzorna_ravnina.Changes) {
		for ev := range ch {
			if err := stream.Send(ev); err != nil {
				return
			}
		}
	}(ch)

	// block to keep the stream open
	select {}
}

// setNodesInternal replaces the full control-plane node list.
// It must be called with the mutex held by the caller OR will lock internally.
func (c *ControlPlaneServer) setNodesInternal(nodes []*NodeInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nodes = nodes
	c.nodeMap = make(map[string]*NodeInfo)
	now := time.Now()
	for _, n := range nodes {
		// On restore, be conservative: treat nodes as alive until proven otherwise
		// by missing heartbeats.
		n.Alive = true
		if n.LastHB.IsZero() {
			n.LastHB = now
		}
		c.nodeMap[n.NodeID] = n
	}
}
