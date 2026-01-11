package main

import (
	"flag"
	"log"
	"net"
	"strings"

	nadzorna_ravnina "github.com/djagodic/razpravljalnica2/pkg/api/nadzornaRavnina"
	control "github.com/djagodic/razpravljalnica2/pkg/control"
	"google.golang.org/grpc"
)

func main() {
	// gRPC API address for clients/servers (ControlPlane service)
	addr := flag.String("addr", "127.0.0.1:5000", "control plane gRPC address")

	// Raft transport address (Jille raft-grpc-transport; must be reachable by other control-plane nodes)
	raftAddr := flag.String("raft-addr", "127.0.0.1:6000", "raft transport address")

	raftID := flag.String("id", "cp1", "raft node ID")
	dataDir := flag.String("data", "data/cp1", "raft data dir")

	// Only ONE node should use -bootstrap=true.
	bootstrap := flag.Bool("bootstrap", false, "bootstrap raft cluster")

	// Initial peer set used ONLY by the bootstrap node.
	// Format: id=raftAddr,id=raftAddr,...
	peersFlag := flag.String("peers", "cp1=127.0.0.1:6000,cp2=127.0.0.1:6001,cp3=127.0.0.1:6002", "initial raft peers (id=addr,...)")

	flag.Parse()

	grpcServer := grpc.NewServer()

	// Init Control Plane + Raft wrapper
	cpServer := control.NewControlPlaneServer(*addr, *raftAddr)
	raftCP := control.NewRaftControlPlane(cpServer)

	peers := parsePeers(*peersFlag)
	if err := raftCP.StartRaftGRPC(*dataDir, *raftID, *bootstrap, peers); err != nil {
		log.Fatalf("Failed to start Raft: %v", err)
	}

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	nadzorna_ravnina.RegisterControlPlaneServer(grpcServer, raftCP)

	log.Printf("Control Plane running on %s (raft=%s, id=%s, bootstrap=%v)", *addr, *raftAddr, *raftID, *bootstrap)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("gRPC server failed: %v", err)
	}
}

func parsePeers(s string) []control.Peer {
	out := make([]control.Peer, 0)
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			continue
		}
		out = append(out, control.Peer{ID: strings.TrimSpace(kv[0]), Addr: strings.TrimSpace(kv[1])})
	}
	return out
}
