package helper

import (
	"context"
	"net"
	"testing"
	"time"

	razpravljalnica "github.com/djagodic/razpravljalnica2/pkg/api/razpravljalnica"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

//buffcon ustvari in memory connection, da nisi odvisen od portov racunalnika pri testih in networka

const bufSize = 1024 * 1024

// vrne povezavo na server in funkcijo za clean up
func SetupTestServer(t *testing.T, srv razpravljalnica.MessageBoardServer) (*grpc.ClientConn, func()) {
	lis := bufconn.Listen(bufSize)

	s := grpc.NewServer()
	razpravljalnica.RegisterMessageBoardServer(s, srv)

	go func() {
		_ = s.Serve(lis) // never call t.Fatal here
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)

	conn, err := grpc.DialContext(
		ctx,
		"bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithInsecure(),
		//grpc.WithBlock(),
	)
	require.NoError(t, err)

	cleanup := func() {
		conn.Close()
		s.GracefulStop()
		lis.Close()
		cancel()
	}

	return conn, cleanup
}
