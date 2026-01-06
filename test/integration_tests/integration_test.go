package integration_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"runtime"
	"time"

	nadzorna_ravnina "github.com/djagodic/razpravljalnica2/pkg/api/nadzornaRavnina"
	razpravljalnica "github.com/djagodic/razpravljalnica2/pkg/api/razpravljalnica"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// getFreePort asks the OS for an available TCP port and returns it as a string "host:port"
func getFreePort() (string, error) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	defer lis.Close()
	port := lis.Addr().(*net.TCPAddr).Port
	return "127.0.0.1:" + strconv.Itoa(port), nil
}

// Helper: wait for TCP port to be ready
func waitForPort(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for %s", addr)
}

// Helper: start a process and return the cmd
func startProcess(t *testing.T, path string, args []string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(path, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())
	return cmd
}

// Helper: stop process and wait
func stopProcess(cmd *exec.Cmd) {
	if cmd != nil && cmd.Process != nil {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	}
}

// Build a binary from a module directory (e.g. ../../cmd/control)
func buildBinary(t *testing.T, srcDir, outPath string) {
	t.Helper()
	cmd := exec.Command("go", "build", "-o", outPath, srcDir)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("failed to build %s: %v\nOutput:\n%s", srcDir, err, string(out))
	}
}

func dialCP(addr string) (*grpc.ClientConn, nadzorna_ravnina.ControlPlaneClient, error) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	return conn, nadzorna_ravnina.NewControlPlaneClient(conn), nil
}

// Wait until *some* control-plane node answers GetClusterState without NOT_LEADER.
// Returns the leader address.
func waitForLeader(t *testing.T, cpAddrs []string, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		for _, addr := range cpAddrs {
			conn, c, err := dialCP(addr)
			if err != nil {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
			_, err = c.GetClusterState(ctx, &emptypb.Empty{})
			cancel()
			_ = conn.Close()

			if err == nil {
				return addr
			}
			if st, ok := status.FromError(err); ok && st.Code() == codes.FailedPrecondition {
				// NOT_LEADER – ignore and try next
				continue
			}
		}
		time.Sleep(150 * time.Millisecond)
	}

	t.Fatalf("no control-plane leader reachable within %s", timeout)
	return ""
}

func TestIntegrationReplication(t *testing.T) {
	ctx := context.Background()

	// ---- Allocate ports ----
	cp1Addr, err := getFreePort()
	require.NoError(t, err)
	cp2Addr, err := getFreePort()
	require.NoError(t, err)
	cp3Addr, err := getFreePort()
	require.NoError(t, err)

	raft1Addr, err := getFreePort()
	require.NoError(t, err)
	raft2Addr, err := getFreePort()
	require.NoError(t, err)
	raft3Addr, err := getFreePort()
	require.NoError(t, err)

	headAddr, err := getFreePort()
	require.NoError(t, err)
	tailAddr, err := getFreePort()
	require.NoError(t, err)

	cpAddrs := []string{cp1Addr, cp2Addr, cp3Addr}
	peersFlag := fmt.Sprintf("cp1=%s,cp2=%s,cp3=%s", raft1Addr, raft2Addr, raft3Addr)

	binExt := ""
	if runtime.GOOS == "windows" {
		binExt = ".exe"
	}

	// ---- Build binaries ----
	buildBinary(t, "../../cmd/control", "./control"+binExt)
	buildBinary(t, "../../cmd/server", "./server"+binExt)

	// ---- Start control-plane (RAFT) ----
	cp1 := startProcess(t, "./control"+binExt, []string{
		"-addr=" + cp1Addr,
		"-raft-addr=" + raft1Addr,
		"-id=cp1",
		"-data=data/test-cp1",
		"-bootstrap=true",
		"-peers=" + peersFlag,
	})
	defer stopProcess(cp1)

	cp2 := startProcess(t, "./control"+binExt, []string{
		"-addr=" + cp2Addr,
		"-raft-addr=" + raft2Addr,
		"-id=cp2",
		"-data=data/test-cp2",
		"-bootstrap=false",
		"-peers=" + peersFlag,
	})
	defer stopProcess(cp2)

	cp3 := startProcess(t, "./control"+binExt, []string{
		"-addr=" + cp3Addr,
		"-raft-addr=" + raft3Addr,
		"-id=cp3",
		"-data=data/test-cp3",
		"-bootstrap=false",
		"-peers=" + peersFlag,
	})
	defer stopProcess(cp3)

	for _, a := range append(cpAddrs, raft1Addr, raft2Addr, raft3Addr) {
		require.NoError(t, waitForPort(a, 5*time.Second))
	}

	leaderAddr := waitForLeader(t, cpAddrs, 8*time.Second)
	t.Logf("control-plane leader is %s", leaderAddr)

	// ---- Start head and tail data servers (chain replication) ----
	headCmd := startProcess(t, "./server"+binExt, []string{
		"-id=node-1",
		"-addr=" + headAddr,
		"-peers=" + fmt.Sprintf("%s,%s,%s", cp1Addr, cp2Addr, cp3Addr),
	})
	defer stopProcess(headCmd)

	tailCmd := startProcess(t, "./server"+binExt, []string{
		"-id=node-2",
		"-addr=" + tailAddr,
		"-peers=" + fmt.Sprintf("%s,%s,%s", cp1Addr, cp2Addr, cp3Addr),
	})
	defer stopProcess(tailCmd)

	require.NoError(t, waitForPort(headAddr, 5*time.Second))
	require.NoError(t, waitForPort(tailAddr, 5*time.Second))

	// ---- Client: connect to head and tail ----
	headConn, err := grpc.Dial(headAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer headConn.Close()
	headClient := razpravljalnica.NewMessageBoardClient(headConn)

	tailConn, err := grpc.Dial(tailAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer tailConn.Close()
	tailClient := razpravljalnica.NewMessageBoardClient(tailConn)

	// ---- Write on head ----
	u, err := headClient.CreateUser(ctx, &razpravljalnica.CreateUserRequest{Name: "alice"})
	require.NoError(t, err)
	topic, err := headClient.CreateTopic(ctx, &razpravljalnica.CreateTopicRequest{Name: "t1"})
	require.NoError(t, err)

	_, err = headClient.PostMessage(ctx, &razpravljalnica.PostMessageRequest{
		TopicId: topic.Id,
		UserId:  u.Id,
		Text:    "hello",
	})
	require.NoError(t, err)

	// ---- Read on tail (eventually consistent) ----
	require.Eventually(t, func() bool {
		resp, err := tailClient.GetMessages(ctx, &razpravljalnica.GetMessagesRequest{
			TopicId:       topic.Id,
			FromMessageId: 0,
			Limit:         10,
		})
		if err != nil {
			return false
		}
		return len(resp.Messages) == 1 && resp.Messages[0].Text == "hello"
	}, 5*time.Second, 150*time.Millisecond)
}
