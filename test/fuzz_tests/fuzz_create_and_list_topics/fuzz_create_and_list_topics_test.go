package fuzz_tests

import (
	"context"
	"testing"
	"time"
	"unicode/utf8"

	razpravljalnica "github.com/djagodic/razpravljalnica2/pkg/api/razpravljalnica"
	"github.com/djagodic/razpravljalnica2/pkg/server"
	helper "github.com/djagodic/razpravljalnica2/test"
	"google.golang.org/protobuf/types/known/emptypb"
)

// run with (from root):go test -fuzz=FuzzCreateAndListTopics github.com/djagodic/razpravljalnica/test/fuzz_tests/fuzz_create_and_list_topics
func FuzzCreateAndListTopics(f *testing.F) {
	f.Add("LOTR")
	f.Add("Test")
	f.Add("Življenje")
	f.Add("very-very-very-long-topic-name-with-special-characters-!@#$%^&*()")

	f.Fuzz(func(t *testing.T, topicName string) {
		if !utf8.ValidString(topicName) {
			return
		}
		if topicName == "" {
			return
		}
		if len(topicName) > 256 {
			return
		}

		server := server.NewMessageBoardServer("node-1", true, true)

		conn, cleanup := helper.SetupTestServer(t, server)
		defer cleanup()

		client := razpravljalnica.NewMessageBoardClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		_, err := client.CreateTopic(ctx, &razpravljalnica.CreateTopicRequest{
			Name: topicName,
		})
		if err != nil {
			t.Errorf("CreateTopic failed: %v", err)
			return
		}

		resp, err := client.ListTopics(ctx, &emptypb.Empty{})
		if err != nil {
			t.Errorf("ListTopics failed: %v", err)
			return
		}

		if len(resp.Topics) != 1 {
			t.Errorf("expected 1 topic, got %d", len(resp.Topics))
			return
		}

		topic := resp.Topics[0]
		if topic.Name != topicName {
			t.Errorf("expected name %q, got %q", topicName, topic.Name)
		}
		if topic.Id == 0 {
			t.Errorf("expected non-zero topic ID")
		}
	})
}
