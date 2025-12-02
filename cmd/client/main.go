package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	api "github.com/yourname/razpravljalnica/pkg/api"
	control "github.com/yourname/razpravljalnica/pkg/control"
	"google.golang.org/grpc"
)

func getClusterState(controlAddr string) (*api.NodeInfo, *api.NodeInfo, error) {
	conn, err := grpc.Dial(controlAddr, grpc.WithInsecure())
	if err != nil {
		return nil, nil, err
	}
	defer conn.Close()

	ctrlClient := control.NewControlPlaneClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := ctrlClient.GetClusterState(ctx, &api.Empty{})
	if err != nil {
		return nil, nil, err
	}

	return resp.Head, resp.Tail, nil
}

func connectToNode(address string) api.MessageBoardClient {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect to node %s: %v", address, err)
	}
	return api.NewMessageBoardClient(conn)
}

func startSubscribe(userID int64, topicIDs []int64, client api.MessageBoardClient) {
	ctx := context.Background()

	// pridobi token za subscription
	subResp, err := client.GetSubcscriptionNode(ctx, &api.SubscriptionNodeRequest{
		UserId:  userID,
		TopicId: topicIDs,
	})
	if err != nil {
		log.Printf("Failed to get subscription node: %v", err)
		return
	}

	subConn, err := grpc.Dial(subResp.Node.Address, grpc.WithInsecure())
	if err != nil {
		log.Printf("Failed to connect to subscription node: %v", err)
		return
	}
	subClient := api.NewMessageBoardClient(subConn)

	stream, err := subClient.SubscribeTopic(ctx, &api.SubscribeTopicRequest{
		UserId:         userID,
		TopicId:        topicIDs,
		FromMessageId:  0,
		SubscribeToken: subResp.SubscribeToken,
	})
	if err != nil {
		log.Printf("Subscribe failed: %v", err)
		return
	}

	go func() {
		for {
			ev, err := stream.Recv()
			if err != nil {
				log.Printf("Subscription ended: %v", err)
				return
			}
			fmt.Printf("[EVENT] %v | Topic %d | User %d: %s (Likes: %d)\n",
				ev.Op, ev.Message.TopicId, ev.Message.UserId, ev.Message.Text, ev.Message.Likes)
		}
	}()
	fmt.Println("Subscription started in background")
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: client <control_plane_addr>")
		return
	}
	controlAddr := os.Args[1]

	head, tail, err := getClusterState(controlAddr)
	if err != nil {
		log.Fatalf("Failed to get cluster state: %v", err)
	}
	fmt.Printf("Head node: %s (%s), Tail node: %s (%s)\n", head.NodeId, head.Address, tail.NodeId, tail.Address)

	headClient := connectToNode(head.Address)
	tailClient := connectToNode(tail.Address)

	reader := bufio.NewReader(os.Stdin)

	var currentUser *api.User

	fmt.Println("Interactive Razpravljalnica CLI")
	fmt.Println("Commands: createuser <name>, createtopic <name>, post <topic_id> <text>, update <topic_id> <msg_id> <text>, delete <topic_id> <msg_id>, like <topic_id> <msg_id>, listtopics, listmessages <topic_id>, subscribe <topic_id1,topic_id2,...>, exit")

	for {
		fmt.Print("> ")
		line, _ := reader.ReadString('\n')
		line = strings.TrimSpace(line)
		parts := strings.SplitN(line, " ", 2)
		if len(parts) == 0 {
			continue
		}
		cmd := strings.ToLower(parts[0])
		args := ""
		if len(parts) > 1 {
			args = parts[1]
		}

		switch cmd {
		case "exit":
			fmt.Println("Exiting CLI")
			return

		case "createuser":
			if args == "" {
				fmt.Println("Usage: createuser <name>")
				continue
			}
			u, err := headClient.CreateUser(context.Background(), &api.CreateUserRequest{Name: args})
			if err != nil {
				fmt.Println("Error creating user:", err)
				continue
			}
			currentUser = u
			fmt.Printf("Created user: %d %s\n", u.Id, u.Name)

		case "createtopic":
			if args == "" {
				fmt.Println("Usage: createtopic <name>")
				continue
			}
			t, err := headClient.CreateTopic(context.Background(), &api.CreateTopicRequest{Name: args})
			if err != nil {
				fmt.Println("Error creating topic:", err)
				continue
			}
			fmt.Printf("Created topic: %d %s\n", t.Id, t.Name)

		case "post":
			if currentUser == nil {
				fmt.Println("No user, please createuser first")
				continue
			}
			fields := strings.SplitN(args, " ", 2)
			if len(fields) < 2 {
				fmt.Println("Usage: post <topic_id> <text>")
				continue
			}
			topicID, _ := strconv.ParseInt(fields[0], 10, 64)
			text := fields[1]
			msg, err := headClient.PostMessage(context.Background(), &api.PostMessageRequest{
				UserId:  currentUser.Id,
				TopicId: topicID,
				Text:    text,
			})
			if err != nil {
				fmt.Println("Error posting message:", err)
				continue
			}
			fmt.Printf("Posted message: %d | %s\n", msg.Id, msg.Text)

		case "update":
			if currentUser == nil {
				fmt.Println("No user, please createuser first")
				continue
			}
			fields := strings.SplitN(args, " ", 3)
			if len(fields) < 3 {
				fmt.Println("Usage: update <topic_id> <msg_id> <text>")
				continue
			}
			topicID, _ := strconv.ParseInt(fields[0], 10, 64)
			msgID, _ := strconv.ParseInt(fields[1], 10, 64)
			text := fields[2]
			msg, err := headClient.UpdateMessage(context.Background(), &api.UpdateMessageRequest{
				UserId:    currentUser.Id,
				TopicId:   topicID,
				MessageId: msgID,
				Text:      text,
			})
			if err != nil {
				fmt.Println("Error updating message:", err)
				continue
			}
			fmt.Printf("Updated message: %d | %s\n", msg.Id, msg.Text)

		case "delete":
			if currentUser == nil {
				fmt.Println("No user, please createuser first")
				continue
			}
			fields := strings.SplitN(args, " ", 2)
			if len(fields) < 2 {
				fmt.Println("Usage: delete <topic_id> <msg_id>")
				continue
			}
			topicID, _ := strconv.ParseInt(fields[0], 10, 64)
			msgID, _ := strconv.ParseInt(fields[1], 10, 64)
			_, err := headClient.DeleteMessage(context.Background(), &api.DeleteMessageRequest{
				UserId:    currentUser.Id,
				TopicId:   topicID,
				MessageId: msgID,
			})
			if err != nil {
				fmt.Println("Error deleting message:", err)
				continue
			}
			fmt.Println("Message deleted")

		case "like":
			if currentUser == nil {
				fmt.Println("No user, please createuser first")
				continue
			}
			fields := strings.SplitN(args, " ", 2)
			if len(fields) < 2 {
				fmt.Println("Usage: like <topic_id> <msg_id>")
				continue
			}
			topicID, _ := strconv.ParseInt(fields[0], 10, 64)
			msgID, _ := strconv.ParseInt(fields[1], 10, 64)
			msg, err := headClient.LikeMessage(context.Background(), &api.LikeMessageRequest{
				UserId:    currentUser.Id,
				TopicId:   topicID,
				MessageId: msgID,
			})
			if err != nil {
				fmt.Println("Error liking message:", err)
				continue
			}
			fmt.Printf("Message liked: %d | Likes: %d\n", msg.Id, msg.Likes)

		case "listtopics":
			resp, err := tailClient.ListTopics(context.Background(), &api.Empty{})
			if err != nil {
				fmt.Println("Error listing topics:", err)
				continue
			}
			for _, t := range resp.Topics {
				fmt.Printf("%d | %s\n", t.Id, t.Name)
			}

		case "listmessages":
			topicID, _ := strconv.ParseInt(args, 10, 64)
			resp, err := tailClient.GetMessages(context.Background(), &api.GetMessagesRequest{
				TopicId:       topicID,
				FromMessageId: 0,
				Limit:         100,
			})
			if err != nil {
				fmt.Println("Error getting messages:", err)
				continue
			}
			for _, m := range resp.Messages {
				fmt.Printf("%d | User %d | %s | Likes: %d\n", m.Id, m.UserId, m.Text, m.Likes)
			}

		case "subscribe":
			if currentUser == nil {
				fmt.Println("No user, please createuser first")
				continue
			}
			idStrs := strings.Split(args, ",")
			var topicIDs []int64
			for _, s := range idStrs {
				id, _ := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
				topicIDs = append(topicIDs, id)
			}
			startSubscribe(currentUser.Id, topicIDs, headClient)

		default:
			fmt.Println("Unknown command")
		}
	}
}
