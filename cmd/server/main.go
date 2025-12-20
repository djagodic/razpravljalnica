package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"time"

	control "github.com/djagodic/razpravljalnica/pkg/api/nadzornaRavnina"
	razpravljalnica "github.com/djagodic/razpravljalnica/pkg/api/razpravljalnica"
	"github.com/djagodic/razpravljalnica/pkg/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addr := flag.String("addr", ":50051", "server address")
	addrControl := flag.String("addrControl", "localhost:5000", "control plane address")
	nodeID := flag.String("id", "node-1", "node id")
	//isHead := flag.Bool("head", true, "is head")
	//isTail := flag.Bool("tail", false, "is tail")
	flag.Parse()

	conn, err := grpc.NewClient(*addrControl, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	ctrlClient := control.NewControlPlaneClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	//naredimo Register node request
	req := &control.RegisterNodeRequest{
		NodeId:  *nodeID,
		Address: *addr,
	}

	resp, err := ctrlClient.RegisterNode(ctx, req)
	if err != nil {
		log.Fatalf("RegisterNode RPC failed: %v", err)
	}

	//preverimo response
	if resp.Success {
		log.Printf("Node registered successfully: %s\n", resp.Message)
	} else {
		log.Printf("Failed to register node: %s\n", resp.Message)
	}

	//glede na response doloci head in tail
	isHead := &resp.IsHead
	isTail := &resp.IsTail

	//naredimo nov grpc strezik
	s := grpc.NewServer()

	//board je struktura za strezenje metod na razpravljalnici
	board := server.NewMessageBoardServer(*nodeID, *isHead, *isTail)

	//strezenje metod na board povezemo s streznikom s
	razpravljalnica.RegisterMessageBoardServer(s, board)

	// izpišemo ime strežnika
	hostName, err := os.Hostname()
	if err != nil {
		panic(err)
	}

	//odpremo vticnico
	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen failed: %v", err)
	}

	log.Printf("gRPC server listening at %v%v\n", hostName, addr)

	log.Printf("Starting node %s at %s (head=%v, tail=%v)", *nodeID, *addr, *isHead, *isTail)

	//zacnemo s strezenjem
	if err := s.Serve(lis); err != nil {
		log.Fatalf("serve failed: %v", err)
	}

}
