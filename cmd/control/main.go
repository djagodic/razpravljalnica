// go run cmd/control/main.go
// build with: go build -o ./bin/control ./cmd/control
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
	addr := flag.String("addr", "localhost:50050", "control address")
	flag.Parse()

	// 2. TCP listener
	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", *addr, err)
	}

	// 3. gRPC strežnik
	grpcServer := grpc.NewServer()

	// 4. Inicializacija ControlPlane
	controlPlane := control.NewControlPlaneServer()

	// 5. Registracija gRPC servisa
	nadzorna_ravnina.RegisterControlPlaneServer(grpcServer, controlPlane)

	// 6. Zagon monitoringa
	controlPlane.Start()

	log.Printf("Control Plane running on %s", *addr)

	// 7. Serve (blocking)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("gRPC server failed: %v", err)
	}
}
