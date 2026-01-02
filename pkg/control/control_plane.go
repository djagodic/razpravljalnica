package control

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	nadzorna_ravnina "github.com/djagodic/razpravljalnica/pkg/api/nadzornaRavnina"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// ControlPlaneServer implementira api.ControlPlane
type ControlPlaneServer struct {
	nadzorna_ravnina.UnimplementedControlPlaneServer

	mu           sync.RWMutex
	nodes        []*NodeInfo // urejena veriga: glava -> ... -> rep
	nodeMap      map[string]*NodeInfo
	interval     time.Duration
	subToChanges map[string]chan *nadzorna_ravnina.Changes // narocniki na spremembe -> kanal
}

type NodeInfo struct {
	NodeID  string
	Address string
	Alive   bool
	LastHB  time.Time
}

// NewControlPlaneServer ustvari novo instanco nadzorne ravnine
func NewControlPlaneServer() *ControlPlaneServer {
	return &ControlPlaneServer{
		nodes:        []*NodeInfo{},
		nodeMap:      make(map[string]*NodeInfo),
		interval:     5 * time.Second, // heartbeat interval
		subToChanges: make(map[string]chan *nadzorna_ravnina.Changes),
	}
}

// RegisterNode doda novo vozlisce v verigo (kliče vozlisce ob zagonu)
// TODO uredi da register node tudi vrne podatke, ki jih mora dati nov node v bazo!
func (c *ControlPlaneServer) RegisterNode(ctx context.Context, req *nadzorna_ravnina.RegisterNodeRequest) (*nadzorna_ravnina.RegisterNodeResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for idx, nodeInfo := range c.nodes {
		// node je ze registriran
		if nodeInfo.NodeID == req.NodeId {
			log.Printf("node %s already registered, at idx %d", req.NodeId, idx)
			// doloci head, tail
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

	// node se ni registriran
	node := &NodeInfo{
		NodeID:  req.NodeId,
		Address: req.Address,
		Alive:   true,
		LastHB:  time.Now(),
	}
	// dodamo nov node na konec in v nodeMap
	c.nodes = append(c.nodes, node)
	c.nodeMap[req.NodeId] = node
	log.Printf("registered node: %s (%s)", req.NodeId, req.Address)

	// doloci head in tail
	isHead := false
	isTail := true
	if len(c.nodes) == 1 { // ce je to edini node je tudi head
		isHead = true
	}

	// preveri ali moras predhodnje node obvestiti
	if len(c.nodes) >= 2 {
		predzadnji := c.nodes[len(c.nodes)-2]
		zadnji := c.nodes[len(c.nodes)-1]
		// posljemu prejsnjemu repu obvestilo o novem repu
		err := c.sendChanges(predzadnji, zadnji, false)
		if err != nil {
			return &nadzorna_ravnina.RegisterNodeResponse{
				Success: false,
				Message: "change at registration failed",
				IsHead:  isHead,
				IsTail:  isTail,
			}, fmt.Errorf("Change at registration nodeId: %s failed", node.NodeID)
		}
	}

	return &nadzorna_ravnina.RegisterNodeResponse{
		Success: true,
		Message: "registered node sucessfully",
		IsHead:  isHead,
		IsTail:  isTail,
	}, nil
}

// DeregisterNode odstrani vozlisce iz verige
func (c *ControlPlaneServer) DeregisterNode(nodeID string) {
	fmt.Printf("DEBUG: deregister node %s\n", nodeID)

	// zaklenili smo ze v hartbeatu
	//c.mu.Lock()
	//defer c.mu.Unlock()
	_, ok := c.nodeMap[nodeID]
	// ce server ni v node Map pac ni registriran in vrnes nic
	if !ok {
		return
	}

	// ga izbrisemo iz map
	delete(c.nodeMap, nodeID)

	// ga izbrisemo iz nodes
	for i, n := range c.nodes {
		if n.NodeID == nodeID {
			if i != 0 && i != len(c.nodes)-1 { // ce ni bil prvi in je za njim bil se en, moramo sprociti predhodniku o spremembi
				fmt.Printf("DEBUG: sent sendchanges v %s in %s\n", c.nodes[i-1].NodeID, c.nodes[i+1].NodeID)
				c.sendChanges(c.nodes[i-1], c.nodes[i+1], false)
			} else if i != 0 && i == len(c.nodes)-1 { // ce ni prvi, ampak je zadnji
				fmt.Printf("DEBUG: sent sendchanges v %s in 'nil'\n", c.nodes[i-1].NodeID)
				c.sendChanges(c.nodes[i-1], nil, false)
			} else if i == 0 { // ce odpove head
				// TODO obvestimo cliente -> NE, client bo sam sel na controlplane ponovno in vzel nov naslov heada
				c.sendChanges(c.nodes[i+1], nil, true)
				fmt.Printf("DEBUG: head down\n")
			}
			// izbrisemo iz verige
			c.nodes = append(c.nodes[:i], c.nodes[i+1:]...)

			break
		}
	}

	// TODO razporedimo njegove subscriberje na druga vozlisca
	// David: vprasal sem Davorja in je rekel, da ce povezava pade bo pac client sou se enkrat vprasat na control plane kam se mora na novo subscribat
	log.Printf("deregistered node: %s", nodeID)
}

// heartbeat posodobi cas zadnjega stika
func (c *ControlPlaneServer) Heartbeat(ctx context.Context, req *nadzorna_ravnina.HeartbeatRequest) (*emptypb.Empty, error) {

	c.mu.Lock()
	defer c.mu.Unlock()

	n, ok := c.nodeMap[req.NodeId]
	if !ok {
		log.Printf("heartbeat from unknown node %s", req.NodeId)
		return &emptypb.Empty{}, nil
	}

	n.LastHB = time.Now()
	n.Alive = true

	return &emptypb.Empty{}, nil
}

// periodicni pregled za zaznavanje mrtvih vozlisc
func (c *ControlPlaneServer) monitorNodes() {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for range ticker.C {
		c.mu.Lock()

		now := time.Now()
		for _, n := range c.nodes {
			if !n.Alive {
				continue
			}
			if now.Sub(n.LastHB) > 2*c.interval {
				n.Alive = false
				log.Printf("node %s marked DEAD", n.NodeID)
				c.reconfigureChain(n.NodeID)
			}

			log.Printf("node %s heartbeat successful", n.NodeID)
		}

		c.mu.Unlock()
	}
}

// ponovna konfiguracija verige po odpovedi vozlisca
func (c *ControlPlaneServer) reconfigureChain(deadNodeID string) {
	log.Printf("reconfiguring chain, removing dead node %s", deadNodeID)
	c.DeregisterNode(deadNodeID)
	// opomba: v polni implementaciji bi obvestili tudi glavo/rep, da posodobita nextNode
}

// GetClusterState vrne trenutno glavo in rep
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

	// pridobi ziva vozlisca
	var alive []*NodeInfo
	for _, n := range c.nodes {
		if n.Alive {
			alive = append(alive, n)
		}
	}

	// preveri ce je sploh kako vozlisce zivo
	if len(alive) == 0 {
		return nil, status.Error(
			codes.Unavailable,
			"no alive nodes available",
		)
	}

	// na podlagi userID, in topicId kamor zelis biti subscriban doloci na kateri node bos poslan
	sum := req.UserId
	for _, t := range req.TopicId {
		sum += t
	}
	idx := sum % int64(len(alive))
	node := alive[idx]

	// token za preverjanje dovoljenj za subscription
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

// start zazene nadzorno zanko
func (c *ControlPlaneServer) Start() {
	go c.monitorNodes()
}

// poslji spremembe narocnikom -> poslji trenutnemu obvestilo o novem naslednjem
func (s *ControlPlaneServer) sendChanges(trenuten, naslednji *NodeInfo, isNewHead bool) error {
	var change *nadzorna_ravnina.Changes
	if naslednji == nil {
		if isNewHead {
			// posebno sporocilo ki pove serverju da je on glava
			change = &nadzorna_ravnina.Changes{NextAdress: "NewHead1234"}
		} else {
			// ce hocemo nastaviti naslednjega na nil bomo poslali prazen string
			change = &nadzorna_ravnina.Changes{NextAdress: ""}
		}

	} else {
		// sporocilo o spremembi
		change = &nadzorna_ravnina.Changes{NextAdress: naslednji.Address}
	}

	// pridobimo kanal predzadnjega, dodana bralna ključavnica
	//s.mu.RLock()
	ch := s.subToChanges[trenuten.NodeID]
	//s.mu.RUnlock()

	// chatko shit, pomoje nepotrebno
	if ch == nil {
		return nil
	}

	select {
	case ch <- change:
	default:
		fmt.Printf("DEBUG: subscriber %s is slow", trenuten.NodeID) // spusti, ce je narocnik pocasen
	}

	return nil
}

// grpc SubscribeToChanges
func (s *ControlPlaneServer) SubscribeToChanges(req *nadzorna_ravnina.SubscribeToChangesRequest, stream nadzorna_ravnina.ControlPlane_SubscribeToChangesServer) error {
	ch := make(chan *nadzorna_ravnina.Changes, 10)

	// dodal sem zaklepanje med nastavljanjem channela za nextNode
	// s.mu.Lock()
	s.subToChanges[req.NodeId] = ch
	// s.mu.Unlock()

	// ko bo vse skupaj crashnilo zbrišem kanal
	// defer func() {
	//     s.mu.Lock()
	//     delete(s.subToChanges, req.NodeId)
	//     close(ch)
	//     s.mu.Unlock()
	// }()

	// posiljanje novih sporocil
	go func(ch chan *nadzorna_ravnina.Changes) {
		for ev := range ch {
			if err := stream.Send(ev); err != nil {
				return
			}
		}
	}(ch)

	// blokiramo, da ostane stream odprt
	select {}
}
