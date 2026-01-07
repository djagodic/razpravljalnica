package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	nadzorna_ravnina "github.com/djagodic/razpravljalnica2/pkg/api/nadzornaRavnina"
	razpravljalnica "github.com/djagodic/razpravljalnica2/pkg/api/razpravljalnica"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

/* ===================== GLOBALNE ===================== */
var (
	// -addrControl lahko vsebuje enega ali več CP naslovov, npr:
	// 127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002
	controlAddr  string
	controlPeers []string

	leaderMu     sync.RWMutex
	cachedLeader string

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

// topicID -> zadnji message ID, ki ga user videl
var lastSeenMessageID = make(map[int64]int64)

/* ===================== THEME (LIGHT UI) ===================== */

func applyLightTheme() {
	// Globalne barve (ozadje belo, tekst temen)
	tview.Styles.PrimitiveBackgroundColor = tcell.ColorWhite
	tview.Styles.ContrastBackgroundColor = tcell.NewRGBColor(235, 235, 235) // rahlo sivo
	tview.Styles.MoreContrastBackgroundColor = tcell.NewRGBColor(220, 220, 220)

	tview.Styles.PrimaryTextColor = tcell.ColorBlack
	tview.Styles.SecondaryTextColor = tcell.NewRGBColor(70, 70, 70)
	tview.Styles.TertiaryTextColor = tcell.NewRGBColor(110, 110, 110)
	tview.Styles.InverseTextColor = tcell.ColorBlack

	// Borders / naslovi
	tview.Styles.BorderColor = tcell.NewRGBColor(120, 120, 120)
	tview.Styles.TitleColor = tcell.NewRGBColor(30, 30, 30)

	// Graphics / selection hint
	tview.Styles.GraphicsColor = tcell.NewRGBColor(120, 120, 120)
}

/* ===================== CONTROL PLANE CONNECT (LEADER-AWARE) ===================== */

func splitComma(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func isNotLeaderErr(err error) bool {
	if err == nil {
		return false
	}
	st, ok := grpcstatus.FromError(err)
	if !ok {
		return false
	}
	// NOT_LEADER tipično pride kot FailedPrecondition
	if st.Code() == codes.FailedPrecondition && strings.Contains(strings.ToUpper(st.Message()), "NOT_LEADER") {
		return true
	}
	return false
}

func isRetryableCpErr(err error) bool {
	// NOT_LEADER => takoj probaj drugega
	if isNotLeaderErr(err) {
		return true
	}
	if err == nil {
		return false
	}
	st, ok := grpcstatus.FromError(err)
	if !ok {
		// network err ipd.
		return true
	}
	// Unavailable/DeadlineExceeded se pogosto pojavljata pri failoverju
	return st.Code() == codes.Unavailable || st.Code() == codes.DeadlineExceeded
}

func setCachedLeader(addr string) {
	leaderMu.Lock()
	defer leaderMu.Unlock()
	cachedLeader = addr
}

func getCachedLeader() string {
	leaderMu.RLock()
	defer leaderMu.RUnlock()
	return cachedLeader
}

// Dial CP node (ne nujno leader) z timeoutom, vrne conn + client
func dialCp(addr string, timeout time.Duration) (*grpc.ClientConn, nadzorna_ravnina.ControlPlaneClient, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, nil, err
	}
	return conn, nadzorna_ravnina.NewControlPlaneClient(conn), nil
}

// Najdi leaderja tako, da na vsak CP poskusi GetClusterState.
// Leader je tisti, ki ne vrne NOT_LEADER in uspešno odgovori.
func findLeader(timeout time.Duration) (string, error) {
	// 1) najprej poskusi cached leader
	if cl := getCachedLeader(); cl != "" {
		conn, client, err := dialCp(cl, timeout)
		if err == nil {
			defer conn.Close()
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			_, err = client.GetClusterState(ctx, &emptypb.Empty{})
			if err == nil {
				return cl, nil
			}
		}
		// cached leader ni več ok -> počisti
		setCachedLeader("")
	}

	// 2) poskusi vse peer-e
	var lastErr error
	for _, addr := range controlPeers {
		conn, client, err := dialCp(addr, timeout)
		if err != nil {
			lastErr = err
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		_, callErr := client.GetClusterState(ctx, &emptypb.Empty{})
		cancel()
		conn.Close()

		if callErr == nil {
			setCachedLeader(addr)
			return addr, nil
		}

		lastErr = callErr
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("no control-plane peers configured")
	}
	return "", fmt.Errorf("no reachable leader: %w", lastErr)
}

// Generic helper: izvedi CP klic na leaderju; ob napaki (NOT_LEADER/Unavailable/DeadlineExceeded)
// poskusi še enkrat z novo leader detekcijo.
func withLeaderClient[T any](timeout time.Duration, fn func(ctx context.Context, client nadzorna_ravnina.ControlPlaneClient) (T, error)) (T, error) {
	var zero T

	leader, err := findLeader(timeout)
	if err != nil {
		return zero, err
	}

	callOnce := func(leaderAddr string) (T, error) {
		conn, client, derr := dialCp(leaderAddr, timeout)
		if derr != nil {
			return zero, derr
		}
		defer conn.Close()

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		return fn(ctx, client)
	}

	res, err := callOnce(leader)
	if err == nil {
		return res, nil
	}

	if !isRetryableCpErr(err) {
		return zero, err
	}

	// failover retry: ponovno najdi leaderja in poskusi še enkrat
	setCachedLeader("")
	leader2, err2 := findLeader(timeout)
	if err2 != nil {
		return zero, err
	}

	return callOnce(leader2)
}

/* ===================== gRPC HELPERS ===================== */

func getClusterState() (*nadzorna_ravnina.NodeInfo, *nadzorna_ravnina.NodeInfo, error) {
	type pair struct {
		head *nadzorna_ravnina.NodeInfo
		tail *nadzorna_ravnina.NodeInfo
	}

	out, err := withLeaderClient[pair](4*time.Second, func(ctx context.Context, client nadzorna_ravnina.ControlPlaneClient) (pair, error) {
		resp, err := client.GetClusterState(ctx, &emptypb.Empty{})
		if err != nil {
			return pair{}, err
		}
		return pair{head: resp.Head, tail: resp.Tail}, nil
	})
	if err != nil {
		return nil, nil, err
	}
	return out.head, out.tail, nil
}

func getSubscriptionNode(ctx context.Context, req *nadzorna_ravnina.SubscriptionNodeRequest) (*nadzorna_ravnina.SubscriptionNodeResponse, error) {
	type wrapper struct {
		resp *nadzorna_ravnina.SubscriptionNodeResponse
	}
	out, err := withLeaderClient[wrapper](4*time.Second, func(_ context.Context, client nadzorna_ravnina.ControlPlaneClient) (wrapper, error) {
		cctx, cancel := context.WithTimeout(ctx, 4*time.Second)
		defer cancel()

		resp, err := client.GetSubscriptionNode(cctx, req)
		if err != nil {
			return wrapper{}, err
		}
		return wrapper{resp: resp}, nil
	})
	if err != nil {
		return nil, err
	}
	return out.resp, nil
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

		head, _, err := getClusterState()
		if err != nil {
			app.Stop()
			fmt.Printf("Failed to connect to control plane to get cluster state\n")
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

	// Light look
	form.SetBackgroundColor(tcell.ColorWhite)
	form.SetFieldBackgroundColor(tcell.NewRGBColor(235, 235, 235))
	form.SetFieldTextColor(tcell.ColorBlack)
	form.SetButtonBackgroundColor(tcell.NewRGBColor(230, 230, 230))
	form.SetButtonTextColor(tcell.ColorBlack)
	form.SetLabelColor(tcell.NewRGBColor(30, 30, 30))

	return form
}

/* ===================== MAIN UI ===================== */

func mainUI() tview.Primitive {
	topics := tview.NewList()
	topics.SetBorder(true)
	topics.SetTitle("Topics")
	topics.SetHighlightFullLine(true)

	// Light list style
	topics.SetMainTextColor(tcell.ColorBlack)
	topics.SetSecondaryTextColor(tcell.NewRGBColor(90, 90, 90))
	topics.SetSelectedTextColor(tcell.ColorBlack)
	topics.SetSelectedBackgroundColor(tcell.NewRGBColor(210, 210, 210))
	topics.SetBackgroundColor(tcell.ColorWhite)

	// uporabim List namesto TextView, da lahko izbiram sporocila
	messages := tview.NewList()
	messages.SetBorder(true)
	messages.SetTitle("Messages")
	messages.SetHighlightFullLine(true)

	messages.SetMainTextColor(tcell.ColorBlack)
	messages.SetSecondaryTextColor(tcell.NewRGBColor(90, 90, 90))
	messages.SetSelectedTextColor(tcell.ColorBlack)
	messages.SetSelectedBackgroundColor(tcell.NewRGBColor(210, 210, 210))
	messages.SetBackgroundColor(tcell.ColorWhite)

	input := tview.NewInputField()
	input.SetLabel("Message: ")
	input.SetFieldWidth(0)

	// Light input style (rahlo sivo polje)
	input.SetLabelColor(tcell.NewRGBColor(30, 30, 30))
	input.SetFieldTextColor(tcell.ColorBlack)
	input.SetFieldBackgroundColor(tcell.NewRGBColor(235, 235, 235))
	input.SetBackgroundColor(tcell.ColorWhite)

	help := tview.NewTextView().
		SetText("Shortcuts (Ctrl +): F1=Input, F3=Topics, F4=Messages, L=Like message, R=Refresh, N=createNewTopic, U=updateMessage, D=deleteMessage, S=Subscribe").
		SetTextColor(tcell.NewRGBColor(0, 70, 140)) //.SetBackgroundColor(tcell.ColorWhite)

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

		head, _, err := getClusterState()
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
					loadTopics(topics, messages, input)
				} else {
					subscribeToTopic(topicID, topics, messages, input)
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
	_, tail, err := getClusterState()
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

	_, tail, err := getClusterState()
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
		// bolj berljivo na belem: username modro, likes temno sivo
		label := fmt.Sprintf("[blue]%s[-]: %s [darkgray](❤️ %d)[-]",
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
}

/* ===================== LIKE MESSAGE ===================== */

func likeMessage(messageID int64) {
	if currentTopicID == 0 || currentUser == nil {
		return
	}

	head, _, err := getClusterState()
	if err != nil {
		return
	}

	client, conn := connectToNode(head.Address)
	defer conn.Close()

	_, err = client.LikeMessage(context.Background(),
		&razpravljalnica.LikeMessageRequest{
			UserId:    currentUser.Id,
			TopicId:   currentTopicID,
			MessageId: messageID,
		})
	if err != nil {
		app.Stop()
		log.Fatalf("Error liking message: %s", err)
		return
	}
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

		head, _, err := getClusterState()
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

	// Light look
	form.SetBackgroundColor(tcell.ColorWhite)
	form.SetFieldBackgroundColor(tcell.NewRGBColor(235, 235, 235))
	form.SetFieldTextColor(tcell.ColorBlack)
	form.SetButtonBackgroundColor(tcell.NewRGBColor(230, 230, 230))
	form.SetButtonTextColor(tcell.ColorBlack)
	form.SetLabelColor(tcell.NewRGBColor(30, 30, 30))

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
		head, _, err := getClusterState()
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

	// Light look
	form.SetBackgroundColor(tcell.ColorWhite)
	form.SetFieldBackgroundColor(tcell.NewRGBColor(235, 235, 235))
	form.SetFieldTextColor(tcell.ColorBlack)
	form.SetButtonBackgroundColor(tcell.NewRGBColor(230, 230, 230))
	form.SetButtonTextColor(tcell.ColorBlack)
	form.SetLabelColor(tcell.NewRGBColor(30, 30, 30))

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
				head, _, err := getClusterState()
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

	// Light look
	modal.SetBackgroundColor(tcell.ColorWhite)
	modal.SetTextColor(tcell.ColorBlack)
	modal.SetButtonBackgroundColor(tcell.NewRGBColor(230, 230, 230))
	modal.SetButtonTextColor(tcell.ColorBlack)

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

	// LEADER-AWARE: GetSubscriptionNode mora na leaderja
	subResp, err := getSubscriptionNode(ctx, &nadzorna_ravnina.SubscriptionNodeRequest{
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

		for {
			ev, err := stream.Recv()
			if err != nil {
				delete(activeSubscriptions, topicID)
				return
			}

			lastReceivedMessageID[topicID] = ev.Message.Id

			app.QueueUpdateDraw(func() {
				if topicID == currentTopicID {
					// ce gledam -> posodobi UI
					currentMessages = append(currentMessages, ev.Message)

					label := fmt.Sprintf("[blue]%s[-]: %s [darkgray](❤️ %d)[-]",
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
	flag.StringVar(&controlAddr, "addrControl", "127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002", "control plane address(es), comma-separated")
	flag.Parse()

	controlPeers = splitComma(controlAddr)
	if len(controlPeers) == 0 {
		log.Fatal("no control plane addresses provided")
	}

	app = tview.NewApplication()
	applyLightTheme()

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