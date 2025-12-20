package control

import (
	"context"
	"log"
	"sync"
	"time"

	nadzorna_ravnina "github.com/djagodic/razpravljalnica/pkg/api/nadzornaRavnina"

	"google.golang.org/protobuf/types/known/emptypb"
)

// ControlPlaneServer implements api.ControlPlane
// TODO spremeni nodes v Linked List
type ControlPlaneServer struct {
	nadzorna_ravnina.UnimplementedControlPlaneServer

	mu       sync.RWMutex
	nodes    []*NodeInfo // ordered chain: head -> ... -> tail
	nodeMap  map[string]*NodeInfo
	interval time.Duration
}

type NodeInfo struct {
	NodeID  string
	Address string
	Alive   bool
	LastHB  time.Time
}

// NewControlPlaneServer creates a new control plane instance
func NewControlPlaneServer() *ControlPlaneServer {
	return &ControlPlaneServer{
		nodes:    []*NodeInfo{},
		nodeMap:  make(map[string]*NodeInfo),
		interval: 5 * time.Second, // heartbeat interval
	}
}

//

// RegisterNode adds a new node to the chain (called by node at startup)
func (c *ControlPlaneServer) RegisterNode(ctx context.Context, req *nadzorna_ravnina.RegisterNodeRequest) (*nadzorna_ravnina.RegisterNodeResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for idx, nodeInfo := range c.nodes {
		//node je ze registriran
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
	c.nodes = append(c.nodes, node)
	c.nodeMap[req.Address] = node
	log.Printf("registered node: %s (%s)", req.NodeId, req.Address)

	//doloci head in tail
	isHead := false
	isTail := true
	if len(c.nodes) == 1 { //ce je to edini node je tudi head
		isHead = true
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
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.nodeMap[nodeID]
	if !ok {
		return
	}
	delete(c.nodeMap, nodeID)
	for i, n := range c.nodes {
		if n.NodeID == nodeID {
			c.nodes = append(c.nodes[:i], c.nodes[i+1:]...)
			break
		}
	}
	log.Printf("deregistered node: %s", nodeID)
}

// Heartbeat updates last seen timestamp
func (c *ControlPlaneServer) Heartbeat(nodeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n, ok := c.nodeMap[nodeID]; ok {
		n.LastHB = time.Now()
		n.Alive = true
	}
}

// Periodic health check to detect dead nodes
func (c *ControlPlaneServer) monitorNodes() {
	ticker := time.NewTicker(c.interval)
	for range ticker.C {
		c.mu.Lock()
		for _, n := range c.nodes {
			if time.Since(n.LastHB) > 2*c.interval {
				if n.Alive {
					n.Alive = false
					log.Printf("node %s (%s) marked as dead", n.NodeID, n.Address)
					c.reconfigureChain(n.NodeID)
				}
			}
		}
		c.mu.Unlock()
	}
}

// Reconfigure chain after node failure
func (c *ControlPlaneServer) reconfigureChain(deadNodeID string) {
	log.Printf("reconfiguring chain, removing dead node %s", deadNodeID)
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

// Start launches monitoring loop
func (c *ControlPlaneServer) Start() {
	go c.monitorNodes()
}
