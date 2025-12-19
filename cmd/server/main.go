package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"

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

	fmt.Printf("gRPC server listening at %v%v\n", hostName, addr)

	log.Printf("Starting node %s at %s (head=%v, tail=%v)", *nodeID, *addr, *isHead, *isTail)

	//zacnemo s strezenjem
	if err := s.Serve(lis); err != nil {
		log.Fatalf("serve failed: %v", err)
	}
}
