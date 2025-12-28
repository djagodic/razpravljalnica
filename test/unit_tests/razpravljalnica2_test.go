package razpravljalnica_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"

	razpravljalnica "github.com/djagodic/razpravljalnica/pkg/api/razpravljalnica"
	"github.com/djagodic/razpravljalnica/pkg/server"
)

func TestCreateTopicAndListTopicsReplication_RealPorts(t *testing.T) {
	ctx := context.Background()

	// Assign ports
	headAddr := "localhost:5055"
	tailAddr := "localhost:5056"

	// Create servers
	head := server.NewMessageBoardServer("head", true, false)
	tail := server.NewMessageBoardServer("tail", false, true)

	// Start gRPC servers in goroutines
	go func() {
		lis, err := net.Listen("tcp", headAddr)
		if err != nil {
			t.Fatalf("failed to listen on %s: %v", headAddr, err)
		}
		s := grpc.NewServer()
		razpravljalnica.RegisterMessageBoardServer(s, head)
		s.Serve(lis)
		time.Sleep(1 * time.Second)
		head.ConnectToNextNode("localhost:5056")
	}()
	go func() {
		lis, err := net.Listen("tcp", tailAddr)
		if err != nil {
			t.Fatalf("failed to listen on %s: %v", headAddr, err)
		}
		s := grpc.NewServer()
		razpravljalnica.RegisterMessageBoardServer(s, tail)
		s.Serve(lis)
	}()

	// Give servers time to start
	time.Sleep(100 * time.Millisecond)

	// Create clients
	headConn, err := grpc.NewClient(headAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer headConn.Close()
	tailConn, err := grpc.NewClient(tailAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer tailConn.Close()

	headClient := razpravljalnica.NewMessageBoardClient(headConn)
	tailClient := razpravljalnica.NewMessageBoardClient(tailConn)

	// Set next node for replication
	head.ConnectToNextNode("localhost:5056")
	//head.SetNextNode(tailClient)

	// Create topic on head
	topicResp, err := headClient.CreateTopic(ctx, &razpravljalnica.CreateTopicRequest{
		Name: "Replication Test",
	})
	require.NoError(t, err)
	require.NotNil(t, topicResp)

	// Wait/poll for replication
	var found bool
	for i := 0; i < 20; i++ {
		resp, err := tailClient.ListTopics(ctx, &emptypb.Empty{})
		require.NoError(t, err)

		found = false
		for _, tpc := range resp.Topics {
			if tpc.Id == topicResp.Id {
				found = true
				break
			}
		}
		if found {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	require.True(t, found, "topic should replicate to tail")
}
