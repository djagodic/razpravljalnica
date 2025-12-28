package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	nadzorna_ravnina "github.com/djagodic/razpravljalnica/pkg/api/nadzornaRavnina"
	razpravljalnica "github.com/djagodic/razpravljalnica/pkg/api/razpravljalnica"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

func getClusterState(controlAddr string) (*nadzorna_ravnina.NodeInfo, *nadzorna_ravnina.NodeInfo, error) {
	conn, err := grpc.NewClient(controlAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	defer conn.Close()
	ctrlClient := nadzorna_ravnina.NewControlPlaneClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := ctrlClient.GetClusterState(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, nil, err
	}

	return resp.Head, resp.Tail, nil
}

func connectToNode(address string) (razpravljalnica.MessageBoardClient, *grpc.ClientConn) {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to node %s: %v", address, err)
	}
	return razpravljalnica.NewMessageBoardClient(conn), conn
}

func startSubscribe(userID int64, topicIDs []int64, fromMessagesId int64, controlAddr string) {
	ctx := context.Background()

	//povezemo se z nadzorno ravnino
	cpConn, err := grpc.NewClient(controlAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to control plane: %v", err)
	}
	defer cpConn.Close()
	cpClient := nadzorna_ravnina.NewControlPlaneClient(cpConn)

	// 1. Ask the control plane which node to subscribe to
	subResp, err := cpClient.GetSubscriptionNode(ctx, &nadzorna_ravnina.SubscriptionNodeRequest{
		UserId:  userID,
		TopicId: topicIDs,
	})
	if err != nil {
		log.Printf("Failed to get subscription node from control plane: %v", err)
		return
	}

	// 2. Connect to the chosen message board node
	subConn, err := grpc.NewClient(subResp.Node.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Failed to connect to subscription node: %v", err)
		return
	}
	subClient := razpravljalnica.NewMessageBoardClient(subConn)

	// 3. Subscribe to topics
	stream, err := subClient.SubscribeTopic(ctx, &razpravljalnica.SubscribeTopicRequest{
		UserId:         userID,
		TopicId:        topicIDs,
		FromMessageId:  fromMessagesId,
		SubscribeToken: subResp.SubscribeToken,
	})
	if err != nil {
		log.Printf("Subscribe failed: %v", err)
		return
	}

	// 4. Listen for events in background
	go func() {
		for {
			ev, err := stream.Recv()
			if err != nil {
				log.Printf("Subscription ended: \n%v", err)
				return
			}
			fmt.Printf("[EVENT] %v | Topic %d (%s) | User %d (%s): %d (%s) (Likes: %d)\n",
				ev.Op, ev.Message.TopicId, ev.Message.TopicName, ev.Message.UserId, ev.Message.UserName, ev.Message.Id, ev.Message.Text, ev.Message.Likes)
		}
	}()
	fmt.Println("Subscription started in background")
}

// user login funkcija
func loginUser(headClient razpravljalnica.MessageBoardClient) (*razpravljalnica.User, error) {
	reader := bufio.NewReader(os.Stdin)

	fmt.Print("Enter username: ")
	name, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Failed to read username:", err)
		return nil, err
	}

	name = strings.TrimSpace(name)
	if name == "" {
		fmt.Println("Username cannot be empty")
		return nil, errors.New("username cannot be empty")
	}

	// Try to get existing user
	u, err := headClient.GetUser(
		context.Background(),
		&razpravljalnica.GetUserRequest{Name: name},
	)

	if err == nil {
		fmt.Printf("Logged in as: %d %s\n", u.Id, u.Name)
		return u, nil
	}

	// User does not exist → create new one
	u, err = headClient.CreateUser(
		context.Background(),
		&razpravljalnica.CreateUserRequest{Name: name},
	)
	if err != nil {
		fmt.Println("Error creating user:", err)
		return u, err
	}

	fmt.Printf("Created and logged in as: %d %s\n", u.Id, u.Name)
	return u, nil
}

func main() {
	//kot argument ob zaganjanju povej address control_plane
	controlAddr := flag.String("addrControl", "localhost:5000", "control plane address")

	//pridobi lokacijo head, tail
	head, tail, err := getClusterState(*controlAddr)
	if err != nil {
		log.Fatalf("Failed to get cluster state: %v", err)
	}
	log.Printf("Head node: %s (%s), Tail node: %s (%s)\n", head.NodeId, head.Address, tail.NodeId, tail.Address)

	//connectaj v head in tail
	headClient, connHead := connectToNode(head.Address)
	// tailClient, connClient := connectToNode(tail.Address)
	//defer connHead.Close()
	//defer connClient.Close()

	reader := bufio.NewReader(os.Stdin)

	fmt.Println("Interactive Razpravljalnica CLI")
	log.Println("Commands:\n createtopic <name>,          post <topic_id> <text>,         update <topic_id> <msg_id> <text>,\n delete <topic_id> <msg_id>,  like <topic_id> <msg_id>,       listtopics,\n listmessages <topic_id>,     subscribe <fromMessageId> <topicId1,topicId2,...>,\n exit")

	//login page
	currentUser, err := loginUser(headClient)
	for err != nil {
		currentUser, err = loginUser(headClient)
	}
	connHead.Close()

	//zacnemo CLI
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

		head, tail, err := getClusterState(*controlAddr)
		if err != nil {
			log.Fatalf("Failed to get cluster state: %v", err)
		}

		switch cmd {
		case "exit":
			fmt.Println("Exiting CLI")
			return

		case "createtopic":
			if args == "" {
				fmt.Println("Usage: createtopic <name>")
				continue
			}
			headClient, connHead := connectToNode(head.Address)
			t, err := headClient.CreateTopic(context.Background(), &razpravljalnica.CreateTopicRequest{Name: args})
			if err != nil {
				fmt.Println("Error creating topic:", err)
				continue
			}
			fmt.Printf("Created topic: %d %s\n", t.Id, t.Name)
			connHead.Close()

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
			headClient, connHead := connectToNode(head.Address)
			msg, err := headClient.PostMessage(context.Background(), &razpravljalnica.PostMessageRequest{
				UserId:  currentUser.Id,
				TopicId: topicID,
				Text:    text,
			})
			if err != nil {
				fmt.Println("Error posting message:", err)
				continue
			}
			fmt.Printf("Posted message: %d | %s\n", msg.Id, msg.Text)
			connHead.Close()

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
			headClient, connHead := connectToNode(head.Address)
			msg, err := headClient.UpdateMessage(context.Background(), &razpravljalnica.UpdateMessageRequest{
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
			connHead.Close()

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
			headClient, connHead := connectToNode(head.Address)
			_, err := headClient.DeleteMessage(context.Background(), &razpravljalnica.DeleteMessageRequest{
				UserId:    currentUser.Id,
				TopicId:   topicID,
				MessageId: msgID,
			})
			if err != nil {
				fmt.Println("Error deleting message:", err)
				continue
			}
			fmt.Println("Message deleted")
			connHead.Close()

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
			headClient, connHead := connectToNode(head.Address)
			msg, err := headClient.LikeMessage(context.Background(), &razpravljalnica.LikeMessageRequest{
				UserId:    currentUser.Id,
				TopicId:   topicID,
				MessageId: msgID,
			})
			if err != nil {
				fmt.Println("Error liking message:", err)
				continue
			}
			fmt.Printf("Message liked: %d (%s)| Likes: %d\n", msg.Id, msg.Text, msg.Likes)
			connHead.Close()

		case "listtopics":
			tailClient, connClient := connectToNode(tail.Address)
			resp, err := tailClient.ListTopics(context.Background(), &emptypb.Empty{})
			if err != nil {
				fmt.Println("Error listing topics:", err)
				continue
			}
			for _, t := range resp.Topics {
				fmt.Printf("%d | %s\n", t.Id, t.Name)
			}
			connClient.Close()

		case "listmessages":
			topicID, _ := strconv.ParseInt(args, 10, 64)
			tailClient, connClient := connectToNode(tail.Address)
			resp, err := tailClient.GetMessages(context.Background(), &razpravljalnica.GetMessagesRequest{
				TopicId:       topicID,
				FromMessageId: 0,
				Limit:         100,
			})
			if err != nil {
				fmt.Println("Error getting messages:", err)
				continue
			}
			for _, m := range resp.Messages {
				fmt.Printf("%d (%s) | User %d (%s) | Message %d (%s) | Likes: %d\n", m.TopicId, m.TopicName, m.UserId, m.UserName, m.Id, m.Text, m.Likes)
			}
			connClient.Close()

		case "subscribe":
			if currentUser == nil {
				fmt.Println("No user, please createuser first")
				continue
			}

			// Expect format: <fromMessageId> <topicId1,topicId2,...>
			fields := strings.Fields(args)
			if len(fields) < 2 {
				fmt.Println("Usage: subscribe <fromMessageId> <topicId1,topicId2,...>")
				continue
			}

			// parse fromMessageId
			fromMessageId, err := strconv.ParseInt(fields[0], 10, 64)
			if err != nil {
				fmt.Println("Invalid fromMessageId:", err)
				continue
			}

			// parse topic IDs
			topicList := strings.Join(fields[1:], " ") // join back the rest in case user typed spaces
			idStrs := strings.Split(topicList, ",")    // split by comma
			var topicIDs []int64
			for _, s := range idStrs {
				s = strings.TrimSpace(s)
				if s == "" {
					continue
				}
				id, err := strconv.ParseInt(s, 10, 64)
				if err != nil {
					fmt.Println("Invalid topic ID:", err)
					continue
				}
				topicIDs = append(topicIDs, id)
			}

			if len(topicIDs) == 0 {
				fmt.Println("No valid topic IDs provided")
				continue
			}

			startSubscribe(currentUser.Id, topicIDs, fromMessageId, *controlAddr)

		default:
			fmt.Println("Unknown command")
		}
	}
}
