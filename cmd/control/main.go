// go run cmd/control/main.go
// build with: go build -o ./bin/control.exe ./cmd/control
package main

import (
	"flag"
	"log"
	"net"

	nadzorna_ravnina "github.com/djagodic/razpravljalnica/pkg/api/nadzornaRavnina"
	control "github.com/djagodic/razpravljalnica/pkg/control"

	"google.golang.org/grpc"
)

func main() {
	// 1. Konfiguracija
	addr := flag.String("addr", "localhost:5000", "control address")
	flag.Parse()

	// 2. TCP poslusanje
	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", *addr, err)
	}

	// gRPC streznik
	grpcServer := grpc.NewServer()

	// inicializacija nadzorne ravnine
	controlPlane := control.NewControlPlaneServer()

	// registracija gRPC servisa
	nadzorna_ravnina.RegisterControlPlaneServer(grpcServer, controlPlane)

	// 6. Zagon monitoringa
	controlPlane.Start()

	log.Printf("Control Plane running on %s", *addr)

	// zacnemo s strezenjem
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("gRPC server failed: %v", err)
	}
}
