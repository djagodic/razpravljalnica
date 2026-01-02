package integration_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	razpravljalnica "github.com/djagodic/razpravljalnica/pkg/api/razpravljalnica"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Helper: start a process and return the cmd
func startProcess(t *testing.T, path string, args []string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(path, args...)
	// redirect output for debugging
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	require.NoError(t, cmd.Start())
	return cmd
}

// Helper: stop process and wait
func stopProcess(cmd *exec.Cmd) {
	if cmd.Process != nil {
		cmd.Process.Kill()
		cmd.Wait()
	}
}

// funkcija za narediti binary
func buildBinary(t *testing.T, srcPath, outPath string) {
	t.Helper()
	cmd := exec.Command("go", "build", "-o", outPath, srcPath)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("failed to build %s: %v\nOutput:\n%s", srcPath, err, string(out))
	}
}

// Helper: wait for TCP port to be ready
func waitForPort(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			conn.Close()
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for %s", addr)
}

// Helper: connect gRPC client to a TCP address
func connectGRPC(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	return conn
}

// getFreePort asks the OS for an available TCP port and returns it as a string "host:port"
func getFreePort() (string, error) {
	lis, err := net.Listen("tcp", "localhost:0") // 0 = pick any free port
	if err != nil {
		return "", err
	}
	defer lis.Close()
	port := lis.Addr().(*net.TCPAddr).Port
	return "localhost:" + strconv.Itoa(port), nil
}

// integration test, ki preveri replikacijo na dveh vozliscih
// ustvarimo head in tail serverja, in clienta, enega ki bere iz head in drugega iz tail
// prvi head client ustvari topic in posta sporocilo na head, tail client pa pricakuje odgovor
func TestIntegrationReplication(t *testing.T) {
	ctx := context.Background()

	// Assign ports
	ctrlAddr, err := getFreePort()
	require.NoError(t, err)

	headAddr, err := getFreePort()
	require.NoError(t, err)

	tailAddr, err := getFreePort()
	require.NoError(t, err)

	//naredimo binarne datoteke
	buildBinary(t, "../../cmd/control", "./control.exe")
	buildBinary(t, "../../cmd/server", "./server.exe")

	// Clean up binaries after test
	defer func() {
		os.Remove("./control.exe")
		os.Remove("./server.exe")
	}()

	// --- 1. Start control plane process ---
	ctrlCmd := startProcess(t, "./control.exe", []string{"-addr=" + ctrlAddr})
	defer stopProcess(ctrlCmd)

	// Wait for control plane to be ready
	require.NoError(t, waitForPort(ctrlAddr, 2*time.Second))
	time.Sleep(50 * time.Millisecond) // allow registration

	// --- 2. Start head and tail servers ---
	headCmd := startProcess(t, "./server.exe", []string{
		"-id=head-1",
		"-addr=" + headAddr,
		"-addrControl=" + ctrlAddr,
	})
	defer stopProcess(headCmd)
	time.Sleep(50 * time.Millisecond) // allow registration

	tailCmd := startProcess(t, "./server.exe", []string{
		"-id=tail-2",
		"-addr=" + tailAddr,
		"-addrControl=" + ctrlAddr,
	})
	defer stopProcess(tailCmd)
	time.Sleep(50 * time.Millisecond) // allow registration

	// Wait for servers to be ready
	require.NoError(t, waitForPort(headAddr, 2*time.Second))
	require.NoError(t, waitForPort(tailAddr, 2*time.Second))

	// --- 3. Connect clients ---
	headConn := connectGRPC(t, headAddr)
	defer headConn.Close()
	tailConn := connectGRPC(t, tailAddr)
	defer tailConn.Close()

	headClient := razpravljalnica.NewMessageBoardClient(headConn)
	//head client mora dejansko biti registriran kot user, ker bo pisal:
	u, err := headClient.CreateUser(
		context.Background(),
		&razpravljalnica.CreateUserRequest{Name: "Frodo"},
	)
	if err != nil {
		fmt.Println("Error creating user:", err)

	}
	tailClient := razpravljalnica.NewMessageBoardClient(tailConn)

	// --- 4. Run subtests sequentially ---
	//spremenljivke da imamo shranjen topic in msg s katerim testiramo
	var topicResp *razpravljalnica.Topic
	var msgResp *razpravljalnica.Message

	t.Run("CreateTopic", func(t *testing.T) {
		topicResp, err = headClient.CreateTopic(ctx, &razpravljalnica.CreateTopicRequest{
			Name: "LOTR debate",
		})
		require.NoError(t, err)
		require.NotNil(t, topicResp)

		// Wait for replication
		require.Eventually(t, func() bool {
			resp, err := tailClient.ListTopics(ctx, &emptypb.Empty{})
			if err != nil {
				return false
			}
			for _, tpc := range resp.Topics {
				if tpc.Id != topicResp.Id {
					return false
				}
			}
			return true
		}, 5*time.Second, 50*time.Millisecond)
	})

	t.Run("PostMessage", func(t *testing.T) {
		msgResp, err = headClient.PostMessage(ctx, &razpravljalnica.PostMessageRequest{
			TopicId: topicResp.Id,
			UserId:  u.Id,
			Text:    "You shall not pass!",
		})
		require.NoError(t, err)
		require.NotNil(t, msgResp)

		require.Eventually(t, func() bool {
			messages, err := tailClient.GetMessages(ctx, &razpravljalnica.GetMessagesRequest{
				TopicId: topicResp.Id,
			})
			if err != nil {
				return false
			}
			for _, m := range messages.Messages {
				if m.Id != msgResp.Id || m.Text != msgResp.Text || m.Likes != 0 {
					return false
				}
			}
			return true
		}, 5*time.Second, 50*time.Millisecond)
	})

	t.Run("LikeMessage", func(t *testing.T) {
		likeResp, err := headClient.LikeMessage(ctx, &razpravljalnica.LikeMessageRequest{
			TopicId:   topicResp.Id,
			MessageId: msgResp.Id,
			UserId:    u.Id,
		})
		require.NoError(t, err)
		require.Equal(t, int32(1), likeResp.Likes)

		// Verify replication
		require.Eventually(t, func() bool {
			messages, err := tailClient.GetMessages(ctx, &razpravljalnica.GetMessagesRequest{
				TopicId: topicResp.Id,
			})
			if err != nil {
				return false
			}
			for _, m := range messages.Messages {
				if m.Id != msgResp.Id || m.Likes != 1 {
					return false
				}
			}
			return true
		}, 5*time.Second, 50*time.Millisecond)
	})

	t.Run("DeleteMessage", func(t *testing.T) {
		_, err := headClient.DeleteMessage(ctx, &razpravljalnica.DeleteMessageRequest{
			TopicId:   topicResp.Id,
			MessageId: msgResp.Id,
			UserId:    u.Id,
		})
		require.NoError(t, err)

		time.Sleep(100 * time.Millisecond) // let replication reach tail

		// Verify deletion on tail
		require.Eventually(t, func() bool {
			messages, err := tailClient.GetMessages(ctx, &razpravljalnica.GetMessagesRequest{
				TopicId: topicResp.Id,
			})
			if err != nil {
				//fmt.Print(err)
				st, ok := status.FromError(err)
				if ok && st.Message() == "no messages for topic" {
					return true //tukaj bo vrnilo true ker bo prazen topic
				}
				return false
			}
			for _, m := range messages.Messages {
				if m.Id == msgResp.Id {
					return false
				}
			}
			return true
		}, 5*time.Second, 50*time.Millisecond)
	})
}

func TestIntegrationReplicationWithMiddleNodeFailure(t *testing.T) {
	ctx := context.Background()

	// Assign ports
	ctrlAddr, err := getFreePort()
	require.NoError(t, err)

	headAddr, err := getFreePort()
	require.NoError(t, err)

	midAddr, err := getFreePort()
	require.NoError(t, err)

	tailAddr, err := getFreePort()
	require.NoError(t, err)

	//naredimo binarne datoteke
	buildBinary(t, "../../cmd/control", "./control.exe")
	buildBinary(t, "../../cmd/server", "./server.exe")

	// Clean up binaries after test
	defer func() {
		os.Remove("./control.exe")
		os.Remove("./server.exe")
	}()

	// --- 1. Start control plane process ---
	ctrlCmd := startProcess(t, "./control.exe", []string{"-addr=" + ctrlAddr})
	defer stopProcess(ctrlCmd)

	// Wait for control plane to be ready
	require.NoError(t, waitForPort(ctrlAddr, 2*time.Second))
	time.Sleep(50 * time.Millisecond) // allow registration

	// --- 2. Start head and mid and tail servers ---
	headCmd := startProcess(t, "./server.exe", []string{
		"-id=head-1",
		"-addr=" + headAddr,
		"-addrControl=" + ctrlAddr,
	})
	defer stopProcess(headCmd)
	time.Sleep(50 * time.Millisecond) // allow registration

	midCmd := startProcess(t, "./server.exe", []string{
		"-id=mid-2",
		"-addr=" + midAddr,
		"-addrControl=" + ctrlAddr,
	})
	time.Sleep(50 * time.Millisecond) // allow registration

	tailCmd := startProcess(t, "./server.exe", []string{
		"-id=tail-3",
		"-addr=" + tailAddr,
		"-addrControl=" + ctrlAddr,
	})
	defer stopProcess(tailCmd)
	time.Sleep(50 * time.Millisecond) // allow registration

	// Wait for servers to be ready
	require.NoError(t, waitForPort(headAddr, 2*time.Second))
	require.NoError(t, waitForPort(tailAddr, 2*time.Second))

	// --- 3. Connect clients to head and tail ---
	headConn := connectGRPC(t, headAddr)
	defer headConn.Close()
	tailConn := connectGRPC(t, tailAddr)
	defer tailConn.Close()

	headClient := razpravljalnica.NewMessageBoardClient(headConn)
	//head client mora dejansko biti registriran kot user, ker bo pisal:
	u, err := headClient.CreateUser(
		context.Background(),
		&razpravljalnica.CreateUserRequest{Name: "Frodo"},
	)
	if err != nil {
		fmt.Println("Error creating user:", err)

	}
	tailClient := razpravljalnica.NewMessageBoardClient(tailConn)

	//naredimo topic in postamo message
	//spremenljivke da imamo shranjen topic in msg s katerim testiramo
	var topicResp *razpravljalnica.Topic
	var msgResp *razpravljalnica.Message
	t.Run("CreateTopic", func(t *testing.T) {
		topicResp, err = headClient.CreateTopic(ctx, &razpravljalnica.CreateTopicRequest{
			Name: "LOTR debate",
		})
		require.NoError(t, err)
		require.NotNil(t, topicResp)

		// Wait for replication
		require.Eventually(t, func() bool {
			resp, err := tailClient.ListTopics(ctx, &emptypb.Empty{})
			if err != nil {
				return false
			}
			for _, tpc := range resp.Topics {
				if tpc.Id != topicResp.Id {
					return false
				}
			}
			return true
		}, 5*time.Second, 50*time.Millisecond)
	})

	t.Run("PostMessage", func(t *testing.T) {
		msgResp, err = headClient.PostMessage(ctx, &razpravljalnica.PostMessageRequest{
			TopicId: topicResp.Id,
			UserId:  u.Id,
			Text:    "You shall not pass!",
		})
		require.NoError(t, err)
		require.NotNil(t, msgResp)

		require.Eventually(t, func() bool {
			messages, err := tailClient.GetMessages(ctx, &razpravljalnica.GetMessagesRequest{
				TopicId: topicResp.Id,
			})
			if err != nil {
				return false
			}
			for _, m := range messages.Messages {
				if m.Id != msgResp.Id || m.Text != msgResp.Text || m.Likes != 0 {
					return false
				}
			}
			return true
		}, 5*time.Second, 50*time.Millisecond)
	})

	t.Run("PostMessageAfterReconfigure", func(t *testing.T) {
		//ubijemo sredinskega
		stopProcess(midCmd)
		time.Sleep(4 * time.Second) //pocakamo 3 s da control zazna da je vozlisce mrtvo

		msgResp, err = headClient.PostMessage(ctx, &razpravljalnica.PostMessageRequest{
			TopicId: topicResp.Id,
			UserId:  u.Id,
			Text:    "What about the 2nd breakfast?",
		})
		require.NoError(t, err)
		require.NotNil(t, msgResp)

		require.Eventually(t, func() bool {
			messages, err := tailClient.GetMessages(ctx, &razpravljalnica.GetMessagesRequest{
				TopicId: topicResp.Id,
			})
			if err != nil {
				fmt.Print(err)
				return false
			}
			for _, m := range messages.Messages {
				if m.Id == msgResp.Id && m.Text == msgResp.Text && m.Likes == 0 {
					return true
				}
			}
			return false
		}, 5*time.Second, 50*time.Millisecond)
	})

}
