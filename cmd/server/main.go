package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	nadzorna_ravnina "github.com/djagodic/razpravljalnica2/pkg/api/nadzornaRavnina"
	razpravljalnica "github.com/djagodic/razpravljalnica2/pkg/api/razpravljalnica"
	"github.com/djagodic/razpravljalnica2/pkg/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

func splitComma(s string) []string { return strings.Split(s, ",") }

// dialLeaderControlPlane returns a connection+client to a reachable CONTROL-PLANE LEADER.
// We detect leadership by calling GetClusterState which is leader-gated in the CP implementation.
func dialLeaderControlPlane(addrs []string) (*grpc.ClientConn, nadzorna_ravnina.ControlPlaneClient, string, error) {
	var lastErr error
	for _, addr := range addrs {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
		cancel()
		if err != nil {
			lastErr = fmt.Errorf("dial %s: %w", addr, err)
			continue
		}
		client := nadzorna_ravnina.NewControlPlaneClient(conn)

		ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
		_, err = client.GetClusterState(ctx2, &emptypb.Empty{})
		cancel2()
		if err != nil {
			_ = conn.Close()
			lastErr = fmt.Errorf("probe leader %s: %w", addr, err)
			continue
		}
		return conn, client, addr, nil
	}
	return nil, nil, "", fmt.Errorf("no reachable leader: %w", lastErr)
}

// registerNode tries to register against the leader; if leader changes it will retry.
func registerNode(nodeID, addr string, cpAddrs []string) (*nadzorna_ravnina.RegisterNodeResponse, error) {
	req := &nadzorna_ravnina.RegisterNodeRequest{NodeId: nodeID, Address: addr}

	for {
		conn, client, used, err := dialLeaderControlPlane(cpAddrs)
		if err != nil {
			return nil, err
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		resp, err := client.RegisterNode(ctx, req)
		cancel()
		_ = conn.Close()

		if err == nil {
			log.Printf("Registered at %s: %+v", used, resp)
			return resp, nil
		}

		log.Printf("RegisterNode failed at %s: %v (retrying)", used, err)
		time.Sleep(500 * time.Millisecond)
	}
}

// startHeartbeat sends heartbeats to the current control-plane leader.
// On errors, it re-selects the leader from cpAddrs.
func startHeartbeat(cpAddrs []string, nodeID string) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		conn, client, used, err := dialLeaderControlPlane(cpAddrs)
		if err != nil {
			log.Printf("heartbeat: no leader reachable: %v", err)
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err = client.Heartbeat(ctx, &nadzorna_ravnina.HeartbeatRequest{NodeId: nodeID})
		cancel()
		_ = conn.Close()

		if err != nil {
			log.Printf("heartbeat failed via %s: %v", used, err)
		}
	}
}

func main() {
	addr := flag.String("addr", ":50051", "node gRPC address")
	nodeID := flag.String("id", "node-1", "node ID")
	peers := flag.String("peers", "127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002", "control plane peers")
	flag.Parse()

	peerList := make([]string, 0)
	for _, p := range splitComma(*peers) {
		p = strings.TrimSpace(p)
		if p != "" {
			peerList = append(peerList, p)
		}
	}

	// 1) DEBUG - Find current leader (for info only)
	_, _, usedLeader, err := dialLeaderControlPlane(peerList)
	if err != nil {
		log.Fatalf("No control-plane leader reachable: %v", err)
	}
	fmt.Println("Using control-plane leader:", usedLeader)

	// 2) Register node (leader may change; register handles retries)
	resp, err := registerNode(*nodeID, *addr, peerList)
	if err != nil {
		log.Fatalf("Failed to register: %v", err)
	}
	isHead := resp.IsHead
	isTail := resp.IsTail

	// 3) Heartbeat (leader-aware)
	go startHeartbeat(peerList, *nodeID)
	fmt.Println("Heartbeat started successfully")

	// 4) Start the message-board gRPC server
	s := grpc.NewServer()
	board := server.NewMessageBoardServer(*nodeID, isHead, isTail)
	razpravljalnica.RegisterMessageBoardServer(s, board)

	// Subscribe to CP changes (leader-aware, auto-reconnect)
	board.StartSubscribingChanges(*nodeID, peerList)

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen failed: %v", err)
	}
	log.Printf("Node %s listening at %s (head=%v, tail=%v)", *nodeID, *addr, isHead, isTail)

	if err := s.Serve(lis); err != nil {
		log.Fatalf("serve failed: %v", err)
	}
}
