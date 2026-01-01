//go run cmd/control/main.go :5000

package main

import (
	"log"
	"net"
	"os"

	nadzorna_ravnina "github.com/djagodic/razpravljalnica/pkg/api/nadzornaRavnina"
	control "github.com/djagodic/razpravljalnica/pkg/control"

	"google.golang.org/grpc"
)

func main() {
	// konfiguracija
	addr := "localhost:5000"
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}

	// TCP poslusanje
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", addr, err)
	}

	// gRPC streznik
	grpcServer := grpc.NewServer()

	// inicializacija nadzorne ravnine
	controlPlane := control.NewControlPlaneServer()

	// registracija gRPC servisa
	nadzorna_ravnina.RegisterControlPlaneServer(grpcServer, controlPlane)

	// zagon monitoringa
	go controlPlane.Start()

	log.Printf("Control Plane running on %s", addr)

	// zacnemo s strezenjem
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("gRPC server failed: %v", err)
	}
}
