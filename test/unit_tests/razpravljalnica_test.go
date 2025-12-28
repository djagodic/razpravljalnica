package razpravljalnica_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	razpravljalnica "github.com/djagodic/razpravljalnica/pkg/api/razpravljalnica"
	"github.com/djagodic/razpravljalnica/pkg/server"
	helper "github.com/djagodic/razpravljalnica/test"
	"google.golang.org/protobuf/types/known/emptypb"
)

// testiraj CreateTopic in ListTopics
func TestCreateTopicAndListTopics(t *testing.T) {
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

	//ustvarimo topic
	_, err := client.CreateTopic(ctx, &razpravljalnica.CreateTopicRequest{
		Name: "Razprave o LOTR in smislu žvljenja",
	})
	require.NoError(t, err)

	//listamo topics
	resp, err := client.ListTopics(ctx, &emptypb.Empty{})
	require.NoError(t, err)

	//preverimo dolzino resp.Topics
	require.Len(t, resp.Topics, 1)

	//preverimo vsebino resp.Topics
	topic := resp.Topics[0]
	require.Equal(t, "Razprave o LOTR in smislu žvljenja", topic.Name)
	require.Equal(t, int64(1), topic.Id)
}

// testiraj post message
func TestPostMessage(t *testing.T) {
	//strukt testov
	tests := []struct {
		name           string
		setup          func(client razpravljalnica.MessageBoardClient) (topicId, userId int64)
		request        *razpravljalnica.PostMessageRequest
		expectError    bool
		errorSubstring string
		validate       func(t *testing.T, msg *razpravljalnica.Message)
	}{
		{
			name: "success",
			setup: func(client razpravljalnica.MessageBoardClient) (int64, int64) {
				ctx := context.Background()

				//ustvarimo userja
				user, err := client.CreateUser(ctx, &razpravljalnica.CreateUserRequest{
					Name: "Frodo",
				})
				require.NoError(t, err)

				//ustvarimo topic
				topic, err := client.CreateTopic(ctx, &razpravljalnica.CreateTopicRequest{
					Name: "LOTR",
				})
				require.NoError(t, err)

				return topic.Id, user.Id
			},
			request: &razpravljalnica.PostMessageRequest{
				Text: "One ring to rule them all", //sporocilo ki ga zelimo poslati
			},
			expectError: false, //ne pricakujemo errorja
			validate: func(t *testing.T, msg *razpravljalnica.Message) {
				require.Equal(t, int64(1), msg.Id)
				require.Equal(t, "LOTR", msg.TopicName)
				require.Equal(t, "Frodo", msg.UserName)
				require.Equal(t, "One ring to rule them all", msg.Text)
				require.Equal(t, int32(0), msg.Likes)
				require.NotNil(t, msg.CreatedAt)
			},
		},
		{
			name: "topic not found",
			setup: func(client razpravljalnica.MessageBoardClient) (int64, int64) {
				ctx := context.Background()

				user, err := client.CreateUser(ctx, &razpravljalnica.CreateUserRequest{
					Name: "Sam",
				})
				require.NoError(t, err)

				return 999, user.Id
			},
			request: &razpravljalnica.PostMessageRequest{
				Text: "Hello",
			},
			expectError:    true,
			errorSubstring: "topic",
		},
		{
			name: "user not found",
			setup: func(client razpravljalnica.MessageBoardClient) (int64, int64) {
				ctx := context.Background()

				topic, err := client.CreateTopic(ctx, &razpravljalnica.CreateTopicRequest{
					Name: "LOTR",
				})
				require.NoError(t, err)

				return topic.Id, 999
			},
			request: &razpravljalnica.PostMessageRequest{
				Text: "Hello",
			},
			expectError:    true,
			errorSubstring: "user",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Arrange (fresh server per subtest)
			server := server.NewMessageBoardServer("node-1", true, true)
			conn, cleanup := helper.SetupTestServer(t, server)
			defer cleanup()

			//ustvarimo clienta
			client := razpravljalnica.NewMessageBoardClient(conn)
			ctx := context.Background()

			topicId, userId := tc.setup(client)

			req := &razpravljalnica.PostMessageRequest{
				TopicId: topicId,
				UserId:  userId,
				Text:    tc.request.Text,
			}

			// klicemo post message
			msg, err := client.PostMessage(ctx, req)

			// Assert
			if tc.expectError {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errorSubstring)
				return
			}

			require.NoError(t, err)
			tc.validate(t, msg)
		})
	}
}

// testiraj update message
func TestUpdateMessage(t *testing.T) {
	// Arrange: create server and client
	nodeId := "node-1"
	isHead := true
	isTail := true
	server := server.NewMessageBoardServer(nodeId, isHead, isTail)

	conn, cleanup := helper.SetupTestServer(t, server)
	defer cleanup()

	client := razpravljalnica.NewMessageBoardClient(conn)
	ctx := context.Background()

	// Shared setup: create user and topic
	userResp, err := client.CreateUser(ctx, &razpravljalnica.CreateUserRequest{Name: "Frodo"})
	require.NoError(t, err)
	require.NotZero(t, userResp.Id)

	topicResp, err := client.CreateTopic(ctx, &razpravljalnica.CreateTopicRequest{Name: "LOTR"})
	require.NoError(t, err)
	require.NotZero(t, topicResp.Id)

	// Shared setup: post a message
	messageResp, err := client.PostMessage(ctx, &razpravljalnica.PostMessageRequest{
		TopicId: topicResp.Id,
		UserId:  userResp.Id,
		Text:    "One ring to rule them all",
	})
	require.NoError(t, err)
	require.NotZero(t, messageResp.Id)

	// Subtest 1: normal update
	t.Run("normal update", func(t *testing.T) {
		updatedText := "One ring to rule them all (edited)"
		updateResp, err := client.UpdateMessage(ctx, &razpravljalnica.UpdateMessageRequest{
			TopicId:   topicResp.Id,
			MessageId: messageResp.Id,
			UserId:    userResp.Id,
			Text:      updatedText,
		})
		require.NoError(t, err)
		require.NotNil(t, updateResp)

		// Assert fields
		require.Equal(t, messageResp.Id, updateResp.Id)
		require.Equal(t, topicResp.Id, updateResp.TopicId)
		require.Equal(t, "LOTR", updateResp.TopicName)
		require.Equal(t, userResp.Id, updateResp.UserId)
		require.Equal(t, "Frodo", updateResp.UserName)
		require.Equal(t, updatedText, updateResp.Text)
		require.NotNil(t, updateResp.CreatedAt)
		require.WithinDuration(t, time.Now(), updateResp.CreatedAt.AsTime(), time.Second)
	})

	// Subtest 2: unauthorized user
	t.Run("unauthorized user", func(t *testing.T) {
		_, err := client.UpdateMessage(ctx, &razpravljalnica.UpdateMessageRequest{
			TopicId:   topicResp.Id,
			MessageId: messageResp.Id,
			UserId:    userResp.Id + 1, // not the author
			Text:      "Hacked text",
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "user not authorized")
	})

	// Subtest 3: message does not exist
	t.Run("message does not exist", func(t *testing.T) {
		_, err := client.UpdateMessage(ctx, &razpravljalnica.UpdateMessageRequest{
			TopicId:   topicResp.Id,
			MessageId: messageResp.Id + 9999, // non-existent message
			UserId:    userResp.Id,
			Text:      "Does not exist",
		})
		require.Error(t, err)
		// You may want to match the exact error from storage
		require.Contains(t, err.Error(), "not found")
	})
}

func TestLikeMessage(t *testing.T) {
	// Arrange: create server and test client
	nodeId := "node-1"
	isHead := true
	isTail := true
	server := server.NewMessageBoardServer(nodeId, isHead, isTail)

	conn, cleanup := helper.SetupTestServer(t, server)
	defer cleanup()

	client := razpravljalnica.NewMessageBoardClient(conn)
	ctx := context.Background()

	// Shared setup: create user and topic
	userResp, err := client.CreateUser(ctx, &razpravljalnica.CreateUserRequest{Name: "Sam"})
	require.NoError(t, err)
	require.NotZero(t, userResp.Id)

	topicResp, err := client.CreateTopic(ctx, &razpravljalnica.CreateTopicRequest{Name: "LOTR"})
	require.NoError(t, err)
	require.NotZero(t, topicResp.Id)

	// Shared setup: post a message
	messageResp, err := client.PostMessage(ctx, &razpravljalnica.PostMessageRequest{
		TopicId: topicResp.Id,
		UserId:  userResp.Id,
		Text:    "One ring to rule them all",
	})
	require.NoError(t, err)
	require.NotZero(t, messageResp.Id)

	// Subtest 1: normal like
	t.Run("normal like", func(t *testing.T) {
		likeResp, err := client.LikeMessage(ctx, &razpravljalnica.LikeMessageRequest{
			TopicId:   topicResp.Id,
			MessageId: messageResp.Id,
			UserId:    userResp.Id,
		})
		require.NoError(t, err)
		require.NotNil(t, likeResp)

		// Assert fields
		require.Equal(t, messageResp.Id, likeResp.Id)
		require.Equal(t, topicResp.Id, likeResp.TopicId)
		require.Equal(t, "LOTR", likeResp.TopicName)
		require.Equal(t, userResp.Id, likeResp.UserId)
		require.Equal(t, "Sam", likeResp.UserName)
		require.Equal(t, "One ring to rule them all", likeResp.Text)
		require.Equal(t, int32(1), likeResp.Likes) // first like
		require.NotNil(t, likeResp.CreatedAt)
		require.WithinDuration(t, time.Now(), likeResp.CreatedAt.AsTime(), time.Second)

		// Like the same message again (simulate multiple likes)
		likeResp2, err := client.LikeMessage(ctx, &razpravljalnica.LikeMessageRequest{
			TopicId:   topicResp.Id,
			MessageId: messageResp.Id,
			UserId:    userResp.Id,
		})
		require.NoError(t, err)
		require.Equal(t, int32(2), likeResp2.Likes)
	})

	// Subtest 2: message does not exist
	t.Run("message does not exist", func(t *testing.T) {
		_, err := client.LikeMessage(ctx, &razpravljalnica.LikeMessageRequest{
			TopicId:   topicResp.Id,
			MessageId: messageResp.Id + 9999, // non-existent message
			UserId:    userResp.Id,
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "not found")
	})
}

func TestDeleteMessage(t *testing.T) {
	// Arrange: create server and test client
	nodeId := "node-1"
	isHead := true
	isTail := true
	server := server.NewMessageBoardServer(nodeId, isHead, isTail)

	conn, cleanup := helper.SetupTestServer(t, server)
	defer cleanup()

	client := razpravljalnica.NewMessageBoardClient(conn)
	ctx := context.Background()

	// Shared setup: create user and topic
	userResp, err := client.CreateUser(ctx, &razpravljalnica.CreateUserRequest{Name: "Frodo"})
	require.NoError(t, err)
	require.NotZero(t, userResp.Id)

	topicResp, err := client.CreateTopic(ctx, &razpravljalnica.CreateTopicRequest{Name: "LOTR"})
	require.NoError(t, err)
	require.NotZero(t, topicResp.Id)

	// Shared setup: post a message
	messageResp, err := client.PostMessage(ctx, &razpravljalnica.PostMessageRequest{
		TopicId: topicResp.Id,
		UserId:  userResp.Id,
		Text:    "One ring to rule them all",
	})
	require.NoError(t, err)
	require.NotZero(t, messageResp.Id)

	// Subtest 1: normal delete
	t.Run("normal delete", func(t *testing.T) {
		resp, err := client.DeleteMessage(ctx, &razpravljalnica.DeleteMessageRequest{
			TopicId:   topicResp.Id,
			MessageId: messageResp.Id,
			UserId:    userResp.Id,
		})
		require.NoError(t, err)
		require.IsType(t, &emptypb.Empty{}, resp)

		// Verify message no longer exists
		_, err = client.UpdateMessage(ctx, &razpravljalnica.UpdateMessageRequest{
			TopicId:   topicResp.Id,
			MessageId: messageResp.Id,
			UserId:    userResp.Id,
			Text:      "Should fail",
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "not found")
	})

	// Subtest 2: unauthorized user
	t.Run("unauthorized user", func(t *testing.T) {
		// Post a new message for this test
		newMsg, err := client.PostMessage(ctx, &razpravljalnica.PostMessageRequest{
			TopicId: topicResp.Id,
			UserId:  userResp.Id,
			Text:    "Another message",
		})
		require.NoError(t, err)

		_, err = client.DeleteMessage(ctx, &razpravljalnica.DeleteMessageRequest{
			TopicId:   topicResp.Id,
			MessageId: newMsg.Id,
			UserId:    userResp.Id + 1, // unauthorized
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "user not authorized")
	})

	// Subtest 3: message does not exist
	t.Run("message does not exist", func(t *testing.T) {
		_, err := client.DeleteMessage(ctx, &razpravljalnica.DeleteMessageRequest{
			TopicId:   topicResp.Id,
			MessageId: 99999, // non-existent message
			UserId:    userResp.Id,
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "not found")
	})
}

func TestSubscribeTopic(t *testing.T) {
	// Arrange: create server and in-memory bufconn client
	nodeId := "node-1"
	isHead := true
	isTail := true
	server := server.NewMessageBoardServer(nodeId, isHead, isTail)

	conn, cleanup := helper.SetupTestServer(t, server)
	defer cleanup()

	client := razpravljalnica.NewMessageBoardClient(conn)
	ctx := context.Background()

	// Shared setup: create user and topic
	userResp, err := client.CreateUser(ctx, &razpravljalnica.CreateUserRequest{Name: "Legolas"})
	require.NoError(t, err)
	topicResp, err := client.CreateTopic(ctx, &razpravljalnica.CreateTopicRequest{Name: "LOTR"})
	require.NoError(t, err)

	// Post a message to have a past message
	pastMsg, err := client.PostMessage(ctx, &razpravljalnica.PostMessageRequest{
		TopicId: topicResp.Id,
		UserId:  userResp.Id,
		Text:    "First message",
	})
	require.NoError(t, err)

	t.Run("receives past messages", func(t *testing.T) {
		req := &razpravljalnica.SubscribeTopicRequest{
			UserId:        userResp.Id,
			TopicId:       []int64{topicResp.Id},
			FromMessageId: 0,
		}

		stream, err := client.SubscribeTopic(ctx, req)
		require.NoError(t, err)

		// Receive the past message
		msgEvent, err := stream.Recv()
		require.NoError(t, err)
		require.Equal(t, pastMsg.Text, msgEvent.Message.Text)

		// Cancel the context to stop the stream goroutine
		_, cancel := context.WithCancel(ctx)
		cancel()
		_ = stream.CloseSend()
	})

	t.Run("receives new messages after subscription", func(t *testing.T) {
		req := &razpravljalnica.SubscribeTopicRequest{
			UserId:        userResp.Id,
			TopicId:       []int64{topicResp.Id},
			FromMessageId: 1,
		}

		stream, err := client.SubscribeTopic(ctx, req)
		require.NoError(t, err)

		received := make(chan *razpravljalnica.MessageEvent, 10)

		// Start a goroutine to receive messages from the stream
		go func() {
			for {
				msg, err := stream.Recv()
				if err != nil {
					close(received)
					return
				}
				received <- msg
			}
		}()

		// Post a new message, should be broadcasted to subscriber
		newMsg, err := client.PostMessage(ctx, &razpravljalnica.PostMessageRequest{
			TopicId: topicResp.Id,
			UserId:  userResp.Id,
			Text:    "Second message",
		})
		require.NoError(t, err)

		// Wait for the new message (ignore old messages)
		timeout := time.After(time.Second)
		receivedMsg := make(chan *razpravljalnica.MessageEvent, 1)
		// anonymous function to wait for the specific message
		func() {
			for {
				select {
				case ev := <-received:
					if ev.Message.Id == newMsg.Id {
						require.Equal(t, newMsg.Text, ev.Message.Text)
						receivedMsg <- ev // pass it out if needed
						return            // exits the anonymous function
					}
				case <-timeout:
					t.Fatal("new message was not received via subscription")
				}
			}
		}()

		// Clean up
		_ = stream.CloseSend()
	})

	t.Run("subscription with non-existent topic returns error", func(t *testing.T) {
		req := &razpravljalnica.SubscribeTopicRequest{
			UserId:        userResp.Id,
			TopicId:       []int64{99999}, // non-existent topic
			FromMessageId: 0,
		}

		//dobimo stream, po katerem bo poslan error topic not found
		stream, err := client.SubscribeTopic(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, stream)

		// The server sends the error immediately on Recv()
		_, err = stream.Recv()
		require.Error(t, err)
		require.Contains(t, err.Error(), "topic not found")
	})
}
