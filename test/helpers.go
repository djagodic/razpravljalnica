package helper

import (
	"context"
	"net"
	"testing"

	razpravljalnica "github.com/djagodic/razpravljalnica/pkg/api/razpravljalnica"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

//buffcon ustvari in memory connection, da nisi odvisen od portov racunalnika pri testih in networka

const bufSize = 1024 * 1024

// vrne povezavo na server in funkcijo za clean up
func SetupTestServer(t *testing.T, srv razpravljalnica.MessageBoardServer) (*grpc.ClientConn, func()) {
	//z bufconn ustvarimo streznik ki bo s clientom povezan preko memory-ja ne preko porta -> se izognemo network tezavam
	lis := bufconn.Listen(bufSize)

	s := grpc.NewServer()
	razpravljalnica.RegisterMessageBoardServer(s, srv)

	//zazenemo server v gorutini
	go func() {
		if err := s.Serve(lis); err != nil {
			t.Fatalf("server exited: %v", err)
		}
	}()

	//ustvarimo povezavo clienta, povezanega na ta server
	ctx := context.Background()
	conn, err := grpc.DialContext(
		ctx,
		"bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithInsecure(),
	)
	require.NoError(t, err)

	//funkcija za cleanup
	cleanup := func() {
		conn.Close()
		s.Stop()
	}

	return conn, cleanup
}
