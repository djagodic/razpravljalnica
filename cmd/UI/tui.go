package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sort"
	"time"

	nadzorna_ravnina "github.com/djagodic/razpravljalnica/pkg/api/nadzornaRavnina"
	razpravljalnica "github.com/djagodic/razpravljalnica/pkg/api/razpravljalnica"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

/* ===================== GLOBALNE ===================== */
var (
	controlAddr     string
	currentUser     *razpravljalnica.User
	app             *tview.Application
	currentTopicID  int64
	currentMessages []*razpravljalnica.Message
)

var pages *tview.Pages

// topicID -> preklici subscription
var activeSubscriptions = make(map[int64]context.CancelFunc)

// topicID -> hrani nevidene spremembe
var topicHasUpdates = make(map[int64]bool)

// topicID -> zadnji dobljeni message ID (subscription kazalec)
var lastReceivedMessageID = make(map[int64]int64)

// topicID -> zadnji message ID, ki ga je user videl
var lastSeenMessageID = make(map[int64]int64)

/* ===================== gRPC HELPERS ===================== */
func getClusterState(addr string) (*nadzorna_ravnina.NodeInfo, *nadzorna_ravnina.NodeInfo, error) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	defer conn.Close()

	client := nadzorna_ravnina.NewControlPlaneClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.GetClusterState(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, nil, err
	}

	return resp.Head, resp.Tail, nil
}

func connectToNode(address string) (razpravljalnica.MessageBoardClient, *grpc.ClientConn) {
	conn, err := grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		app.Stop()
		log.Fatalf("Failed to connect to node %s: %v", address, err)
	}
	return razpravljalnica.NewMessageBoardClient(conn), conn
}

/* ===================== UI SCREENS ===================== */
func loginScreen(onSuccess func()) tview.Primitive {
	form := tview.NewForm()
	var username string

	form.AddInputField("Username", "", 20, nil, func(text string) {
		username = text
	})

	form.AddButton("Login", func() {
		if username == "" {
			return
		}

		head, _, err := getClusterState(controlAddr)
		if err != nil {
			app.Stop()
			fmt.Printf("Failed to connect to control plane at %s to get cluster state\n", controlAddr)
			log.Fatal(err)
			return
		}

		client, conn := connectToNode(head.Address)
		defer conn.Close()

		u, err := client.GetUser(context.Background(),
			&razpravljalnica.GetUserRequest{Name: username})

		if err != nil {
			u, err = client.CreateUser(context.Background(),
				&razpravljalnica.CreateUserRequest{Name: username})
			if err != nil {
				app.Stop()
				fmt.Printf("Failed to create user")
				log.Fatal(err)
				return
			}
		}

		currentUser = u
		onSuccess()
	})

	form.SetBorder(true)
	form.SetTitle("Login")

	return form
}

/* ===================== MAIN UI ===================== */
func mainUI() tview.Primitive {
	topics := tview.NewList()
	topics.SetBorder(true)
	topics.SetTitle("Topics")
	topics.SetHighlightFullLine(true)

	// uporabim List namesto TextView, da lahko izbiram sporocila
	messages := tview.NewList()
	messages.SetBorder(true)
	messages.SetTitle("Messages")
	messages.SetHighlightFullLine(true)

	input := tview.NewInputField()
	input.SetLabel("Message: ")
	input.SetFieldWidth(0)

	help := tview.NewTextView().
		SetText("Shortcuts (Ctrl +): F1=Input, F3=Topics, F4=Messages, L=Like message, R=Refresh, N=createNewTopic, U=updateMessage, D=deleteMessage, S=Subscribe").
		SetTextColor(tcell.ColorGreen)

	flex := tview.NewFlex().SetDirection(tview.FlexRow)
	horizontal := tview.NewFlex()
	horizontal.AddItem(topics, 0, 1, true)
	horizontal.AddItem(messages, 0, 3, false)
	flex.AddItem(horizontal, 0, 1, true)
	flex.AddItem(input, 3, 0, false)
	flex.AddItem(help, 2, 0, false)

	loadTopics(topics, messages, input)
	app.SetFocus(topics)

	/* ===================== POSTING ===================== */
	input.SetDoneFunc(func(key tcell.Key) {
		if key != tcell.KeyEnter || currentTopicID == 0 {
			return
		}

		text := input.GetText()
		if text == "" {
			return
		}

		head, _, err := getClusterState(controlAddr)
		if err != nil {
			return
		}

		client, conn := connectToNode(head.Address)
		defer conn.Close()

		_, err = client.PostMessage(context.Background(),
			&razpravljalnica.PostMessageRequest{
				TopicId: currentTopicID,
				Text:    text,
				UserId:  currentUser.Id,
			})
		if err == nil {
			input.SetText("")
			loadMessages(messages, currentTopicID)
			loadTopics(topics, messages, input)
		}
	})

	/* ===================== KEY SHORTCUTS ===================== */
	flex.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Modifiers()&tcell.ModCtrl != 0 {
			switch event.Key() {
			case tcell.KeyF3:
				app.SetFocus(topics)
			case tcell.KeyF4:
				app.SetFocus(messages)
			case tcell.KeyF1:
				app.SetFocus(input)
			case tcell.KeyCtrlR:
				app.SetFocus(topics)
				loadTopics(topics, messages, input)
				if currentTopicID != 0 {
					loadMessages(messages, currentTopicID)
				}
				loadTopics(topics, messages, input)
				app.SetFocus(messages)
			case tcell.KeyCtrlL: // like izbranega sporocila
				index := messages.GetCurrentItem()
				if index >= 0 && index < len(currentMessages) {
					likeMessage(currentMessages[index].Id)
					loadMessages(messages, currentTopicID)
					loadTopics(topics, messages, input)
				}
			case tcell.KeyCtrlN: // Ctrl+N -> naredi novo temo
				createTopicPrompt(topics, messages, input)
				loadTopics(topics, messages, input)
			case tcell.KeyCtrlU:
				updateMessagePrompt(messages, input)
				loadTopics(topics, messages, input)
			case tcell.KeyCtrlD:
    			deleteMessagePrompt(messages, input)
				loadTopics(topics, messages, input)
			case tcell.KeyCtrlS: // Ctrl+S -> subscribe na trenutno izbrano temo
				index := topics.GetCurrentItem()
				if index < 0 {
					break
				}

				itemText, _ := topics.GetItemText(index)
				var topicID int64
				_, err := fmt.Sscanf(itemText, "%d:", &topicID)
				if err != nil {
					break
				}

				if cancel, ok := activeSubscriptions[topicID]; ok {
					cancel()
					delete(activeSubscriptions, topicID)
					//log.Printf("Unsubscribed from topic %d\n", topicID)
					loadTopics(topics, messages, input)
				} else {
					subscribeToTopic(topicID, topics, messages, input) // od trenutnega message Id naprej delam subscribe
					loadTopics(topics, messages, input)
				}
			}
		}
		return event
	})

	pages = tview.NewPages().
    AddPage("main", flex, true, true)

	return pages

}

/* ===================== TOPIC LOADING ===================== */
func loadTopics(topics, messages *tview.List, input *tview.InputField) {
	selected := topics.GetCurrentItem()
	_, tail, err := getClusterState(controlAddr)
	if err != nil {
		return
	}

	client, conn := connectToNode(tail.Address)
	defer conn.Close()

	resp, err := client.ListTopics(context.Background(), &emptypb.Empty{})
	if err != nil {
		return
	}

	// sortiraj teme po Id
	sort.Slice(resp.Topics, func(i, j int) bool {
		return resp.Topics[i].Id < resp.Topics[j].Id
	})

	topics.Clear()
	for _, t := range resp.Topics {
		topicID := t.Id

		label := fmt.Sprintf("%d: %s", t.Id, t.Name)

		if _, ok := activeSubscriptions[t.Id]; ok {
			if lastReceivedMessageID[t.Id] > lastSeenMessageID[t.Id] {
				label = fmt.Sprintf("%s  [green]S*[-] 🔔", label)
			} else {
				label = fmt.Sprintf("%s  [green]S[-]", label)
			}
		}

		topics.AddItem(
			label,
			"",
			0,
			func() {
				currentTopicID = topicID
				loadMessages(messages, topicID)
				app.SetFocus(input) // po izbiri teme dej fokus na input
			},
		)
	}

	if selected >= 0 && selected < topics.GetItemCount() {
		topics.SetCurrentItem(selected)
	}

}

/* ===================== LOAD MESSAGES ===================== */
func loadMessages(list *tview.List, topicID int64) {
	list.Clear()
	currentMessages = nil

	_, tail, err := getClusterState(controlAddr)
	if err != nil {
		return
	}

	client, conn := connectToNode(tail.Address)
	defer conn.Close()

	resp, err := client.GetMessages(context.Background(),
		&razpravljalnica.GetMessagesRequest{
			TopicId: topicID,
			Limit:   100,
		})
	if err != nil {
		return
	}

	currentMessages = resp.Messages

	// user zdej gleda -> pocisti obvestila
	topicHasUpdates[topicID] = false

	for _, m := range resp.Messages {
		label := fmt.Sprintf("[yellow]%s[-]: %s [gray](❤️ %d)[-]",
			m.UserName, m.Text, m.Likes)

		if m.Id > lastSeenMessageID[topicID] {
			label = fmt.Sprintf("%s [red]NEW[-] ✨", label)
		}

		list.AddItem(label, "", 0, nil)
	}

	// fokus na zadnje sporocilo
	if len(currentMessages) > 0 {
		list.SetCurrentItem(len(currentMessages) - 1)
	}

	if len(resp.Messages) > 0 {
		lastSeenMessageID[topicID] = resp.Messages[len(resp.Messages)-1].Id
	}

	//TODO: poskusi narediti refresh tem tukaj, da se * pobrise ko subscriber pogleda spororocila
}

/* ===================== LIKE MESSAGE ===================== */
func likeMessage(messageID int64) {
	if currentTopicID == 0 || currentUser == nil {
		return
	}

	head, _, err := getClusterState(controlAddr)
	if err != nil {
		return
	}

	client, conn := connectToNode(head.Address)
	defer conn.Close()

	//msg, err := client.LikeMessage(context.Background(),
	_, err = client.LikeMessage(context.Background(),
		&razpravljalnica.LikeMessageRequest{
			UserId:    currentUser.Id,
			TopicId:   currentTopicID, // <- include TopicId
			MessageId: messageID,
		})
	if err != nil {
		app.Stop()
		log.Fatalf("Error liking message: %s", err)
		return
	}

	// printaj liked sporocila v konsolo za debug
	//fmt.Printf("Message liked: %d (%s) | Likes: %d\n", msg.Id, msg.Text, msg.Likes)
}

/* ===================== CREATE TOPIC ===================== */
func createTopicPrompt(topics, messages *tview.List, input *tview.InputField) {
	if currentUser == nil {
		return
	}

	form := tview.NewForm()
	var topicName string

	form.AddInputField("Topic name", "", 30, nil, func(text string) {
		topicName = text
	})

	form.AddButton("Create", func() {
		if topicName == "" {
			return
		}

		head, _, err := getClusterState(controlAddr)
		if err != nil {
			return
		}

		client, conn := connectToNode(head.Address)
		defer conn.Close()

		topic, err := client.CreateTopic(context.Background(), &razpravljalnica.CreateTopicRequest{
			Name: topicName,
		})
		if err != nil {
			return
		}

		currentTopicID = topic.Id
		loadMessages(messages, currentTopicID)
		loadTopics(topics, messages, input)

		pages.RemovePage("create")
		app.SetFocus(input)
	})

	form.AddButton("Cancel", func() {
		pages.RemovePage("create")
		app.SetFocus(input)
	})

	form.SetBorder(true).SetTitle("Create Topic").SetTitleAlign(tview.AlignLeft)

	pages.AddPage("create", form, true, true)
	app.SetFocus(form)
}

/* ===================== UPDATE MESSAGE ===================== */
func updateMessagePrompt(messages *tview.List, input *tview.InputField) {
	index := messages.GetCurrentItem()
	if index < 0 || index >= len(currentMessages) {
		return
	}

	msg := currentMessages[index]
	if currentUser == nil || msg.UserId != currentUser.Id {
		// dovoli posodobit le lastna sporocila
		return
	}

	form := tview.NewForm()
	var newText string = msg.Text

	form.AddInputField("Edit Message", msg.Text, 0, nil, func(text string) {
		newText = text
	})

	form.AddButton("Update", func() {
		head, _, err := getClusterState(controlAddr)
		if err != nil {
			return
		}

		client, conn := connectToNode(head.Address)
		defer conn.Close()

		_, err = client.UpdateMessage(context.Background(), &razpravljalnica.UpdateMessageRequest{
			UserId:    currentUser.Id,
			TopicId:   currentTopicID,
			MessageId: msg.Id,
			Text:      newText,
		})
		if err != nil {
			return
		}

		loadMessages(messages, currentTopicID)
		pages.RemovePage("update")
		app.SetFocus(input)
	})

	form.AddButton("Cancel", func() {
		pages.RemovePage("update")
		app.SetFocus(input)
	})

	form.SetBorder(true).SetTitle("Update Message").SetTitleAlign(tview.AlignLeft)

	pages.AddPage("update", form, true, true)
	app.SetFocus(form)
}


/* ===================== DELETE MESSAGE ===================== */
func deleteMessagePrompt(messages *tview.List, input *tview.InputField) {
	index := messages.GetCurrentItem()
	if index < 0 || index >= len(currentMessages) {
		return
	}

	msg := currentMessages[index]
	if currentUser == nil || msg.UserId != currentUser.Id {
		// dovoli brisat le lastna sporocila
		return
	}

	modal := tview.NewModal().
		SetText("Do you really want to delete this message?").
		AddButtons([]string{"Delete", "Cancel"}).
		SetDoneFunc(func(buttonIndex int, buttonLabel string) {
			if buttonLabel == "Delete" {
				head, _, err := getClusterState(controlAddr)
				if err != nil {
					return
				}

				client, conn := connectToNode(head.Address)
				defer conn.Close()

				_, err = client.DeleteMessage(context.Background(), &razpravljalnica.DeleteMessageRequest{
					UserId:    currentUser.Id,
					TopicId:   currentTopicID,
					MessageId: msg.Id,
				})
				if err != nil {
					return
				}

				loadMessages(messages, currentTopicID)
			}
			// zapri modal v obeh primerih
			pages.RemovePage("delete")
			app.SetFocus(input)
		})

	pages.AddPage("delete", modal, true, true)
	app.SetFocus(modal)
}

/* ===================== SUBSCRIPTION ===================== */
func subscribeToTopic(topicID int64, topics, messagesList *tview.List, input *tview.InputField) {
	if currentUser == nil {
		return
	}

	// preklaplaj unsubscribe
	if cancel, ok := activeSubscriptions[topicID]; ok {
		cancel()
		delete(activeSubscriptions, topicID)
		return
	}

	fromID := lastReceivedMessageID[topicID]

	ctx, cancel := context.WithCancel(context.Background())
	activeSubscriptions[topicID] = cancel

	cpConn, err := grpc.Dial(controlAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		app.Stop()
		log.Fatal(err)
		return
	}

	cpClient := nadzorna_ravnina.NewControlPlaneClient(cpConn)
	subResp, err := cpClient.GetSubscriptionNode(ctx,
		&nadzorna_ravnina.SubscriptionNodeRequest{
			UserId:  currentUser.Id,
			TopicId: []int64{topicID},
		})
	if err != nil {
		app.Stop()
		log.Fatal(err)
		return
	}

	subConn, err := grpc.Dial(subResp.Node.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		app.Stop()
		log.Fatal(err)
		return
	}

	subClient := razpravljalnica.NewMessageBoardClient(subConn)
	stream, err := subClient.SubscribeTopic(ctx,
		&razpravljalnica.SubscribeTopicRequest{
			UserId:         currentUser.Id,
			TopicId:        []int64{topicID},
			FromMessageId:  fromID + 1,
			SubscribeToken: subResp.SubscribeToken,
		})
	if err != nil {
		app.Stop()
		log.Fatal(err)
		return
	}

	go func() {
		defer subConn.Close()
		defer cpConn.Close()

		for {
			ev, err := stream.Recv()
			if err != nil {
				delete(activeSubscriptions, topicID)
				return
			}

			lastReceivedMessageID[topicID] = ev.Message.Id

			app.QueueUpdateDraw(func() {
				if topicID == currentTopicID {
					// ce gledam (trenutna tema je tudi tista na katero subscribam -> posodobi UI
					currentMessages = append(currentMessages, ev.Message)

					label := fmt.Sprintf("[yellow]%s[-]: %s [gray](❤️ %d)[-]",
						ev.Message.UserName,
						ev.Message.Text,
						ev.Message.Likes,
					)

					if ev.Message.Id > lastSeenMessageID[topicID] {
						label = fmt.Sprintf("%s [red]NEW[-] ✨", label)
					}

					messagesList.AddItem(label, "", 0, nil)

					messagesList.SetCurrentItem(len(currentMessages) - 1)

					lastSeenMessageID[topicID] = ev.Message.Id

				} else {
					// ne gledam -> oznaci temo
					topicHasUpdates[topicID] = true
					loadTopics(topics, messagesList, input)
				}
			})
		}
	}()
}


/* ===================== MAIN ===================== */
func main() {
	flag.StringVar(&controlAddr, "addrControl", "localhost:5000", "control plane address")
	flag.Parse()

	app = tview.NewApplication()

	app.SetRoot(
		loginScreen(func() {
			app.SetRoot(mainUI(), true)
		}),
		true,
	)

	if err := app.Run(); err != nil {
		panic(err)
	}
}
