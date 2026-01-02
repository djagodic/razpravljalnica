package main

//build with: go build -o ./bin/server ./cmd/server

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"time"

	nadzorna_ravnina "github.com/djagodic/razpravljalnica/pkg/api/nadzornaRavnina"
	razpravljalnica "github.com/djagodic/razpravljalnica/pkg/api/razpravljalnica"
	"github.com/djagodic/razpravljalnica/pkg/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// zacni heartbeat
func startHeartbeat(cpAddr, nodeID string) {
	conn, err := grpc.NewClient(cpAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("heartbeat dial failed: %v", err)
	}
	client := nadzorna_ravnina.NewControlPlaneClient(conn)

	ticker := time.NewTicker(2 * time.Second)
	for range ticker.C {
		_, err := client.Heartbeat(context.Background(), &nadzorna_ravnina.HeartbeatRequest{NodeId: nodeID})
		if err != nil {
			log.Printf("heartbeat failed: %v", err)
		}
	}
}

func main() {
	addr := flag.String("addr", ":50051", "server address")
	addrControl := flag.String("addrControl", "localhost:5000", "control plane address")
	nodeID := flag.String("id", "node-1", "node id")
	//isHead := flag.Bool("head", true, "is head")
	//isTail := flag.Bool("tail", false, "is tail")
	flag.Parse()

	// povezemo se na nadzorno ravnino kot client
	conn, err := grpc.NewClient(*addrControl, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	ctrlClient := nadzorna_ravnina.NewControlPlaneClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// naredimo Register node request
	req := &nadzorna_ravnina.RegisterNodeRequest{
		NodeId:  *nodeID,
		Address: *addr,
	}

	// registriramo node
	resp, err := ctrlClient.RegisterNode(ctx, req)
	if err != nil {
		log.Fatalf("RegisterNode RPC failed: %v", err)
	}

	// preverimo response
	if resp.Success {
		log.Printf("Node registered successfully: %s\n", resp.Message)
	} else {
		log.Printf("Failed to register node: %s\n", resp.Message)
	}

	// glede na response doloci head in tail
	isHead := &resp.IsHead
	isTail := &resp.IsTail

	// zacnemo s hartbeatom
	go startHeartbeat(*addrControl, *nodeID)

	// naredimo nov grpc strezik z clienta ce si head, ali za predhodni server ce si vmes
	s := grpc.NewServer()

	// board je struktura za strezenje metod na razpravljalnici
	board := server.NewMessageBoardServer(*nodeID, *isHead, *isTail)

	// strezenje metod na board povezemo s streznikom s
	razpravljalnica.RegisterMessageBoardServer(s, board)

	// odpri stream in poslušaj za spremembe s strani nadzorne ravnine
	board.StartSubscribingChanges(*nodeID, ctrlClient)

	// izpisemo ime streznika
	hostName, err := os.Hostname()
	if err != nil {
		panic(err)
	}

	// odpremo vticnico
	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen failed: %v", err)
	}

	log.Printf("gRPC server listening at %v%v\n", hostName, addr)

	log.Printf("Starting node %s at %s (head=%v, tail=%v)", *nodeID, *addr, *isHead, *isTail)

	// zacnemo s strezenjem
	if err := s.Serve(lis); err != nil {
		log.Fatalf("serve failed: %v", err)
	}

}
