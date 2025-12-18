package main

import (
	"flag"
	"log"
	"net"

	razpravljalnica "github.com/djagodic/razpravljalnica/pkg/api/razpravljalnica"
	"github.com/djagodic/razpravljalnica/pkg/server"
	"google.golang.org/grpc"
)

func main() {
	addr := flag.String("addr", ":50051", "server address")
	nodeID := flag.String("id", "node-1", "node id")
	isHead := flag.Bool("head", true, "is head")
	isTail := flag.Bool("tail", false, "is tail")
	flag.Parse()

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen failed: %v", err)
	}

	s := grpc.NewServer()
	board := server.NewMessageBoardServer(*nodeID, *isHead, *isTail)
	razpravljalnica.RegisterMessageBoardServer(s, board)

	log.Printf("Starting node %s at %s (head=%v, tail=%v)", *nodeID, *addr, *isHead, *isTail)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("serve failed: %v", err)
	}
}
