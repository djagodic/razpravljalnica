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
	// 1. Konfiguracija
	addr := ":5000"
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}

	// 2. TCP listener
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", addr, err)
	}

	// 3. gRPC strežnik
	grpcServer := grpc.NewServer()

	// 4. Inicializacija ControlPlane
	controlPlane := control.NewControlPlaneServer()

	// 5. Registracija gRPC servisa
	nadzorna_ravnina.RegisterControlPlaneServer(grpcServer, controlPlane)

	// 6. Zagon monitoringa
	controlPlane.Start()

	log.Printf("Control Plane running on %s", addr)

	// 7. Serve (blocking)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("gRPC server failed: %v", err)
	}
}
