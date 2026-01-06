package fuzz_tests

import (
	"context"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"

	razpravljalnica "github.com/djagodic/razpravljalnica2/pkg/api/razpravljalnica"
	"github.com/djagodic/razpravljalnica2/pkg/server"
	helper "github.com/djagodic/razpravljalnica2/test"
)

// run with (from root): go test -fuzz=FuzzPostMessage github.com/djagodic/razpravljalnica/test/fuzz_tests/fuzz_post_message
func FuzzPostMessage(f *testing.F) {
	// Seed corpus
	f.Add("One ring to rule them all")
	f.Add("It's a dangerous business, Frodo, going out your door. You step onto the road, and if you don't keep your feet, there's no knowing where you might be swept off to.")
	f.Add("Življenje")
	f.Add("")

	f.Fuzz(func(t *testing.T, text string) {
		// Skip invalid UTF-8 since protobuf strings must be valid UTF-8
		if !utf8.ValidString(text) {
			return
		}

		// Arrange: fresh server per fuzz iteration
		nodeId := "node-1"
		isHead := true
		isTail := true
		server := server.NewMessageBoardServer(nodeId, isHead, isTail)

		conn, cleanup := helper.SetupTestServer(t, server)
		defer cleanup()

		client := razpravljalnica.NewMessageBoardClient(conn)
		ctx := context.Background()

		// Create user
		userResp, err := client.CreateUser(ctx, &razpravljalnica.CreateUserRequest{
			Name: "Frodo",
		})
		require.NoError(t, err)
		require.NotZero(t, userResp.Id)

		// Create topic
		topicResp, err := client.CreateTopic(ctx, &razpravljalnica.CreateTopicRequest{
			Name: "LOTR",
		})
		require.NoError(t, err)
		require.NotZero(t, topicResp.Id)

		// Act: post message
		msgResp, err := client.PostMessage(ctx, &razpravljalnica.PostMessageRequest{
			TopicId: topicResp.Id,
			UserId:  userResp.Id,
			Text:    text,
		})

		// Assert
		require.NoError(t, err)
		require.NotZero(t, msgResp.Id)
		require.Equal(t, topicResp.Id, msgResp.TopicId)
		require.Equal(t, "LOTR", msgResp.TopicName)
		require.Equal(t, userResp.Id, msgResp.UserId)
		require.Equal(t, "Frodo", msgResp.UserName)
		require.Equal(t, text, msgResp.Text)
		require.Equal(t, int32(0), msgResp.Likes)
		require.NotNil(t, msgResp.CreatedAt)
	})
}
