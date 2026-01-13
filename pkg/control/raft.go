package control

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

	transport "github.com/Jille/raft-grpc-transport"
	nadzorna_ravnina "github.com/djagodic/razpravljalnica2/pkg/api/nadzornaRavnina"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type RaftControlPlane struct {
	*ControlPlaneServer

	raft     *raft.Raft
	raftFSM  *RaftFSM
	raftGRPC *grpc.Server
	raftLis  net.Listener
}

type Peer struct {
	ID   string
	Addr string
}

func NewRaftControlPlane(c *ControlPlaneServer) *RaftControlPlane {
	r := &RaftControlPlane{ControlPlaneServer: c}
	// Wire hooks so ControlPlaneServer can replicate membership changes discovered via Heartbeat monitoring.
	c.leaderCheck = r.IsLeader
	c.applyDeregisterFn = r.ApplyDeregisterNode
	c.applySetNodesFn = r.ApplySetNodes
	return r
}

// IsLeader returns true if this node is Raft leader.
func (r *RaftControlPlane) IsLeader() bool {
	if r.raft == nil {
		return false
	}
	return r.raft.State() == raft.Leader
}

func (r *RaftControlPlane) ensureLeader() error {
	if r.raft == nil {
		return status.Error(codes.Unavailable, "RAFT_NOT_READY")
	}
	if !r.IsLeader() {
		// Keep message stable for existing client/server retry logic.
		return status.Error(codes.FailedPrecondition, "NOT_LEADER")
	}
	return nil
}

// StartRaftGRPC starts hashicorp/raft with the Jille gRPC transport on the provided grpcServer.
//
// bootstrap:
//   - true: bootstrap a new cluster with the provided initialPeers (must include THIS node).
//   - false: just start Raft; membership is expected to be provided by the bootstrapper.
//
// initialPeers is only used when bootstrap=true.
func (r *RaftControlPlane) StartRaftGRPC(dataDir, raftID string, bootstrap bool, initialPeers []Peer) error {
	// Ensure data directory exists
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return fmt.Errorf("[%s] failed to create data directory: %w", raftID, err)
	}

	// Raft configuration
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(raftID)

	// BoltDB log store
	storePath := filepath.Join(dataDir, "raft.db")
	store, err := raftboltdb.NewBoltStore(storePath)
	if err != nil {
		return fmt.Errorf("[%s] failed to create BoltStore: %w", raftID, err)
	}

	// Snapshot store
	snapshots, err := raft.NewFileSnapshotStore(dataDir, 1, nil)
	if err != nil {
		return fmt.Errorf("[%s] failed to create snapshot store: %w", raftID, err)
	}

	// Raft gRPC transport (this address is the one other Raft peers must dial).
	raftTransport := transport.New(
		raft.ServerAddress(r.ControlPlaneServer.raftAddress),
		[]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	)
	// Start a dedicated gRPC server for Raft transport on raftAddress.
	// IMPORTANT: raftAddress must be reachable by other peers (it is NOT the control-plane addr).
	raftSrv := grpc.NewServer()
	raftTransport.Register(raftSrv)
	lis, err := net.Listen("tcp", r.ControlPlaneServer.raftAddress)
	if err != nil {
		return fmt.Errorf("[%s] failed to listen on raftAddress %s: %w", raftID, r.ControlPlaneServer.raftAddress, err)
	}
	r.raftGRPC = raftSrv
	r.raftLis = lis
	go func() {
		if err := raftSrv.Serve(lis); err != nil {
			log.Printf("[%s] Raft gRPC transport server stopped: %v", raftID, err)
		}
	}()

	r.raftFSM = &RaftFSM{c: r.ControlPlaneServer}

	r.raft, err = raft.NewRaft(config, r.raftFSM, store, store, snapshots, raftTransport.Transport())
	if err != nil {
		return fmt.Errorf("[%s] failed to start Raft: %w", raftID, err)
	}

	if bootstrap {
		// Safety: ensure the local peer is present in the config.
		cfg := raft.Configuration{Servers: make([]raft.Server, 0, len(initialPeers))}
		seenLocal := false
		for _, p := range initialPeers {
			if p.ID == "" || p.Addr == "" {
				continue
			}
			cfg.Servers = append(cfg.Servers, raft.Server{
				ID:      raft.ServerID(p.ID),
				Address: raft.ServerAddress(p.Addr),
			})
			if p.ID == raftID || p.Addr == r.ControlPlaneServer.raftAddress {
				seenLocal = true
			}
		}
		if !seenLocal {
			cfg.Servers = append(cfg.Servers, raft.Server{
				ID:      config.LocalID,
				Address: raftTransport.Transport().LocalAddr(),
			})
		}

		future := r.raft.BootstrapCluster(cfg)
		if err := future.Error(); err != nil && err != raft.ErrCantBootstrap {
			return fmt.Errorf("[%s] failed to bootstrap cluster: %w", raftID, err)
		}
		log.Printf("[%s] Raft cluster bootstrapped with %d servers", raftID, len(cfg.Servers))
	}

	// Start monitoring loop (Heartbeat-based failure detection) like before.
	// The loop itself is leader-gated by c.leaderCheck.
	r.ControlPlaneServer.Start()

	return nil
}

// ApplyDeregisterNode replicates a node removal via Raft.
func (r *RaftControlPlane) ApplyDeregisterNode(nodeID string) error {
	if err := r.ensureLeader(); err != nil {
		return err
	}
	cmd := &RPCCommand{Op: "DeregisterNode", NodeID: nodeID}
	data, _ := cmd.Marshal()
	f := r.raft.Apply(data, 5*time.Second)
	return f.Error()
}

// ApplyHeartbeat replicates node heartbeats via Raft so all members share the same liveness view.
func (r *RaftControlPlane) ApplyHeartbeat(nodeID string, hbAt time.Time) error {
	if err := r.ensureLeader(); err != nil {
		return err
	}
	cmd := &RPCCommand{Op: "Heartbeat", NodeID: nodeID, HeartbeatAt: hbAt.UnixNano()}
	data, _ := cmd.Marshal()
	f := r.raft.Apply(data, 5*time.Second)
	return f.Error()
}

// ApplySetNodes replicates the full node list via Raft (used when leader recomputes liveness / ordering).
func (r *RaftControlPlane) ApplySetNodes(nodes []*NodeInfo) error {
	if err := r.ensureLeader(); err != nil {
		return err
	}
	cmd := &RPCCommand{Op: "SetNodes", Nodes: nodes}
	data, _ := cmd.Marshal()
	f := r.raft.Apply(data, 5*time.Second)
	return f.Error()
}

// ---------------- gRPC methods (leader-gated) ----------------

// RegisterNode is a write -> must go through the Raft log.
func (r *RaftControlPlane) RegisterNode(ctx context.Context, req *nadzorna_ravnina.RegisterNodeRequest) (*nadzorna_ravnina.RegisterNodeResponse, error) {
	if err := r.ensureLeader(); err != nil {
		return nil, err
	}

	cmd := &RPCCommand{
		Op:                  "RegisterNode",
		RegisterNodeRequest: req,
	}

	data, _ := cmd.Marshal()
	f := r.raft.Apply(data, 5*time.Second)
	if err := f.Error(); err != nil {
		return nil, err
	}

	// After Apply, leader (and later followers) have the state in-memory.
	// Return from local state to preserve existing behavior (IsHead/IsTail flags, etc.).
	return r.ControlPlaneServer.registerNodeInternal(ctx, req)
}

// Heartbeat updates liveness info. Only the leader should accept it.
func (r *RaftControlPlane) Heartbeat(ctx context.Context, req *nadzorna_ravnina.HeartbeatRequest) (*emptypb.Empty, error) {
	// Heartbeat is a write (liveness update) -> must go through the Raft log.
	if err := r.ensureLeader(); err != nil {
		return nil, err
	}
	if err := r.ApplyHeartbeat(req.NodeId, time.Now()); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (r *RaftControlPlane) GetClusterState(ctx context.Context, _ *emptypb.Empty) (*nadzorna_ravnina.GetClusterStateResponse, error) {
	if err := r.ensureLeader(); err != nil {
		return nil, err
	}
	return r.ControlPlaneServer.GetClusterState(ctx, &emptypb.Empty{})
}

func (r *RaftControlPlane) GetSubscriptionNode(ctx context.Context, req *nadzorna_ravnina.SubscriptionNodeRequest) (*nadzorna_ravnina.SubscriptionNodeResponse, error) {
	if err := r.ensureLeader(); err != nil {
		return nil, err
	}
	return r.ControlPlaneServer.GetSubscriptionNode(ctx, req)
}

func (r *RaftControlPlane) SubscribeToChanges(req *nadzorna_ravnina.SubscribeToChangesRequest, stream nadzorna_ravnina.ControlPlane_SubscribeToChangesServer) error {
	if err := r.ensureLeader(); err != nil {
		return err
	}
	return r.ControlPlaneServer.SubscribeToChanges(req, stream)
}

// GetLeader returns current leader Raft address (best-effort).
func (r *RaftControlPlane) GetLeader() string {
	if r.raft == nil {
		return ""
	}
	addr := r.raft.Leader()
	if addr == "" {
		return ""
	}
	return string(addr)
}

func (r *RaftControlPlane) Raft() *raft.Raft {
	return r.raft
}

// ---------------- Raft FSM ----------------

type RaftFSM struct {
	c *ControlPlaneServer
}

func (f *RaftFSM) Apply(logEntry *raft.Log) interface{} {
	cmd := &RPCCommand{}
	if err := cmd.Unmarshal(logEntry.Data); err != nil {
		log.Printf("failed to unmarshal raft command: %v", err)
		return err
	}

	switch cmd.Op {
	case "RegisterNode":
		_, _ = f.c.registerNodeInternal(context.Background(), cmd.RegisterNodeRequest)
	case "DeregisterNode":
		f.c.DeregisterNode(cmd.NodeID)
	case "SetNodes":
		f.c.setNodesInternal(cmd.Nodes)
	case "Heartbeat":
		f.c.mu.Lock()
		f.c.heartbeatInternalLocked(cmd.NodeID, time.Unix(0, cmd.HeartbeatAt))
		f.c.mu.Unlock()
	default:
		log.Printf("unknown raft op: %s", cmd.Op)
	}
	return nil
}

func (f *RaftFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.c.mu.RLock()
	defer f.c.mu.RUnlock()
	// copy slice to avoid data races; NodeInfo pointers are treated as immutable between snapshots
	cp := make([]*NodeInfo, len(f.c.nodes))
	copy(cp, f.c.nodes)
	return &RaftSnapshot{nodes: cp}, nil
}

func (f *RaftFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	var nodes []*NodeInfo
	if err := json.NewDecoder(rc).Decode(&nodes); err != nil {
		return err
	}
	f.c.setNodesInternal(nodes)
	return nil
}

type RaftSnapshot struct {
	nodes []*NodeInfo
}

func (r *RaftSnapshot) Persist(sink raft.SnapshotSink) error {
	defer sink.Close()
	return json.NewEncoder(sink).Encode(r.nodes)
}

func (r *RaftSnapshot) Release() {}
