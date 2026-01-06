package fuzz_tests

import (
	"context"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"

	razpravljalnica "github.com/djagodic/razpravljalnica2/pkg/api/razpravljalnica"
	"github.com/djagodic/razpravljalnica2/pkg/server"
	helper "github.com/djagodic/razpravljalnica2/test"
	"google.golang.org/protobuf/types/known/emptypb"
)

// run with (from root):go test -fuzz=FuzzCreateAndListTopics github.com/djagodic/razpravljalnica/test/fuzz_tests/fuzz_create_and_list_topics
func FuzzCreateAndListTopics(f *testing.F) {
	//smemena (imamo vec semen, da v bljizini nih iscemo stringe za testirat)
	f.Add("LOTR")
	f.Add("Test")
	f.Add("Življenje")
	f.Add("very-very-very-long-topic-name-with-special-characters-!@#$%^&*()")

	f.Fuzz(func(t *testing.T, topicName string) {
		// Protobuf requires valid UTF-8
		if !utf8.ValidString(topicName) {
			return
		}
		//ustvarimo nov Message board server
		nodeId := "node-1"
		isHead := true
		isTail := true
		server := server.NewMessageBoardServer(nodeId, isHead, isTail)

		//pridobimo povezavo na server in cleanup funkcijo
		conn, cleanup := helper.SetupTestServer(t, server)
		defer cleanup()
		//povezemo se na server
		client := razpravljalnica.NewMessageBoardClient(conn)
		ctx := context.Background()

		// ustvarimo Topic
		_, err := client.CreateTopic(ctx, &razpravljalnica.CreateTopicRequest{
			Name: topicName,
		})

		// ime topica ne sme biti prazen string
		if topicName == "" {
			return
		}
		require.NoError(t, err)

		// ListTopics
		resp, err := client.ListTopics(ctx, &emptypb.Empty{})
		require.NoError(t, err)

		//preverimo
		require.Len(t, resp.Topics, 1)

		topic := resp.Topics[0]
		require.Equal(t, topicName, topic.Name)
		require.NotZero(t, topic.Id)
	})
}
