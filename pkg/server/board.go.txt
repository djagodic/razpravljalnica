package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	nadzorna_ravnina "github.com/djagodic/razpravljalnica2/pkg/api/nadzornaRavnina"
	razpravljalnica "github.com/djagodic/razpravljalnica2/pkg/api/razpravljalnica"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// naj gredo od 1 naprej, lazje uporabnikov, patch da subscribe vrne tudi prvi message (ker z 0 ni hotlo)
var nextUserId int64 = 1
var nextTopicId int64 = 1
var nextMessageId int64 = 1

type MessageBoardServer struct {
	razpravljalnica.UnimplementedMessageBoardServer
	mu sync.RWMutex

	storage *NodeStorage
	log     *ReplicationLog

	seq int64

	nodeId   string
	IsHead   bool
	IsTail   bool
	nextNode razpravljalnica.MessageBoardClient

	// subscription kanali: topicId -> userId -> kanal *api.MessageEvent
	subs map[string]map[string]chan *razpravljalnica.MessageEvent
}

func NewMessageBoardServer(nodeId string, isHead, isTail bool) *MessageBoardServer {
	return &MessageBoardServer{
		storage:  NewNodeStorage(),
		log:      NewReplicationLog(),
		nodeId:   nodeId,
		IsHead:   isHead,
		IsTail:   isTail,
		nextNode: nil,
		subs:     make(map[string]map[string]chan *razpravljalnica.MessageEvent),
	}
}

// helper za testiranje
func (s *MessageBoardServer) SetNextNode(client razpravljalnica.MessageBoardClient) {
	s.nextNode = client
}

// nextSequence vrne monotono zaporedno stevilko
func (s *MessageBoardServer) nextSequence() int64 {
	return atomic.AddInt64(&s.seq, 1)
}

func (s *MessageBoardServer) StartSubscribingChanges(nodeId string, controlPlaneAddrs []string) {
	// Runs forever in background: subscribe to chain changes from the current CP leader.
	go func() {
		for {
			conn, client, used, err := dialLeaderControlPlane(controlPlaneAddrs)
			if err != nil {
				log.Printf("SubscribeToChanges: no reachable control-plane leader: %v", err)
				time.Sleep(1 * time.Second)
				continue
			}

			ctx := context.Background()
			stream, err := client.SubscribeToChanges(ctx, &nadzorna_ravnina.SubscribeToChangesRequest{NodeId: nodeId})
			if err != nil {
				log.Printf("SubscribeToChanges: failed at %s: %v", used, err)
				_ = conn.Close()
				time.Sleep(500 * time.Millisecond)
				continue
			}

			log.Printf("SubscribeToChanges: stream established via leader %s", used)

			for {
				ev, err := stream.Recv()
				if err != nil {
					log.Printf("SubscribeToChanges: stream ended (%s): %v", used, err)
					break
				}
				log.Printf("Next node address received: %s", ev.NextAdress)
				s.ConnectToNextNode(ev.NextAdress)
			}

			_ = conn.Close()
			time.Sleep(500 * time.Millisecond)
		}
	}()
}

// dialLeaderControlPlane tries addresses and returns the first one that answers a leader-only RPC.
func dialLeaderControlPlane(addrs []string) (*grpc.ClientConn, nadzorna_ravnina.ControlPlaneClient, string, error) {
	var lastErr error
	for _, addr := range addrs {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
		cancel()
		if err != nil {
			lastErr = fmt.Errorf("dial %s: %w", addr, err)
			continue
		}
		client := nadzorna_ravnina.NewControlPlaneClient(conn)

		// Probe leadership.
		ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
		_, err = client.GetClusterState(ctx2, &emptypb.Empty{})
		cancel2()
		if err != nil {
			_ = conn.Close()
			lastErr = fmt.Errorf("probe leader %s: %w", addr, err)
			continue
		}

		return conn, client, addr, nil
	}
	return nil, nil, "", lastErr
}


// povezi se na naslednji server v verigi
func (s *MessageBoardServer) ConnectToNextNode(address string) {
	// ce pride posebno sporocilo gremo in nastavimo novi head node -> namesto posebne funkcije uporabimo kar tole
	if address == "NewHead1234" {
		//s.mu.Lock()
		s.IsHead = true
		//s.mu.Unlock()
		log.Printf("We have set up a new head node, the Head state: %t", s.IsHead)
		return
	}

	// povezavo na nil naredimo tako da je address prazen string -> postanemo rep
	if address == "" {
		//s.mu.Lock()
		s.nextNode = nil
		s.IsTail = true
		//s.mu.Unlock()
		log.Printf("Connected to new node: 'nil, status Tail: %t", s.IsTail)
		return
	}

	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to node %s: %v", address, err)
	}
	//s.mu.Lock()
	client := razpravljalnica.NewMessageBoardClient(conn)

	// zamrznemo replikacijo
	s.mu.Lock()
	s.nextNode = nil
	s.IsTail = false
	s.mu.Unlock()

	// zgradimo snapshot
	snap := s.storage.BuildSnapshot()

	// poslji snapshot
	_, err = client.InstallSnapshot(context.Background(), snap)
	if err != nil {
		log.Printf("snapshot failed: %v", err)
		return
	}

	// potrditev je implicitna ob uspesnem RPC klicu
	s.mu.Lock()
	s.nextNode = client
	s.mu.Unlock()

	log.Printf("[%s] Snapshot installed, replication enabled", s.nodeId)
	//s.mu.Unlock()
}

func (s *MessageBoardServer) InstallSnapshot(
	ctx context.Context,
	snap *razpravljalnica.StorageSnapshot,
) (*emptypb.Empty, error) {

	s.mu.Lock()
	defer s.mu.Unlock()

	log.Printf("[%s] Installing snapshot", s.nodeId)

	// ponastavi trenutno shrambo
	s.storage.Reset()

	maxUserId := int64(0)
	maxTopicId := int64(0)
	maxMessageId := int64(0)

	// obnovi uporabnike
	for _, u := range snap.Users {
		s.storage.AddUser(u)
		if u.Id > maxUserId {
			maxUserId = u.Id
		}
	}

	// obnovi teme
	for _, t := range snap.Topics {
		s.storage.AddTopic(t)
		if t.Id > maxTopicId {
			maxTopicId = t.Id
		}
	}

	// obnovi sporocila
	for _, m := range snap.Messages {
		s.storage.AddMessage(m)
		if m.Id > maxMessageId {
			maxMessageId = m.Id
		}
	}

	// posodobi globalne naslednje ID-je
	nextUserId = maxUserId + 1
	nextTopicId = maxTopicId + 1
	nextMessageId = maxMessageId + 1

	log.Printf("[%s] Snapshot installed, nextUserId=%d, nextTopicId=%d, nextMessageId=%d",
		s.nodeId, nextUserId, nextTopicId, nextMessageId)

	return &emptypb.Empty{}, nil
}

func (s *NodeStorage) BuildSnapshot() *razpravljalnica.StorageSnapshot {
	return &razpravljalnica.StorageSnapshot{
		Users:    s.ListUsers(),
		Topics:   s.ListTopics(),
		Messages: s.ListAllMessages(),
	}
}

// ------------------------ gRPC metode -----------------------------
func (s *MessageBoardServer) GetUser(ctx context.Context, req *razpravljalnica.GetUserRequest) (*razpravljalnica.User, error) {

	user := s.storage.GetUserByName(req.Name)
	if user != nil {
		log.Printf("getUser -> %d (%s)", user.Id, user.Name)
		return user, nil
	}

	return nil, errors.New("user does not exist")
}

func (s *MessageBoardServer) CreateUser(ctx context.Context, req *razpravljalnica.CreateUserRequest) (*razpravljalnica.User, error) {
	user := &razpravljalnica.User{
		Id:   nextUserId,
		Name: req.Name,
	}
	s.storage.AddUser(user)

	// povecamo next user Id
	nextUserId++

	// belezenje in replikacija
	entry := &LogEntry{
		Op:       OpCreateUser,
		User:     user,
		Sequence: s.nextSequence(),
	}
	s.log.Add(entry)

	// repllikacija + zaklep branja
	//s.mu.RLock()
	next := s.nextNode
	//s.mu.RUnlock()

	if next != nil {
		_, err := next.CreateUser(ctx, req)
		if err != nil {
			fmt.Print("Error creating user:", err)
			fmt.Printf("CurrentNode: %s", s.nodeId)
		}
	}

	log.Printf("createUser -> %d (%s)", user.Id, user.Name)
	return user, nil
}

func (s *MessageBoardServer) CreateTopic(ctx context.Context, req *razpravljalnica.CreateTopicRequest) (*razpravljalnica.Topic, error) {
	topic := &razpravljalnica.Topic{
		Id:   nextTopicId,
		Name: req.Name,
	}
	s.storage.AddTopic(topic)

	// povecamo next topic id
	nextTopicId++

	entry := &LogEntry{
		Op:       OpCreateTopic,
		Topic:    topic,
		Sequence: s.nextSequence(),
	}
	s.log.Add(entry)

	// replikacija + zaklep branja
	//s.mu.RLock()
	next := s.nextNode
	//s.mu.RUnlock()

	if next != nil {
		_, err := next.CreateTopic(ctx, req)
		if err != nil {
			fmt.Print("Error creating topic:", err)
			fmt.Printf("CurrentNode: %s", s.nodeId)
		}
	}

	log.Printf("createTopic -> %d (%s)", topic.Id, topic.Name)
	return topic, nil
}

func (s *MessageBoardServer) PostMessage(ctx context.Context, req *razpravljalnica.PostMessageRequest) (*razpravljalnica.Message, error) {
	topic := s.storage.GetTopicById(req.TopicId)
	if topic == nil {
		return nil, fmt.Errorf("topic %d not found", req.TopicId)
	}

	user := s.storage.GetUserById(req.UserId)
	if user == nil {
		return nil, fmt.Errorf("user %d not found", req.UserId)
	}

	message := &razpravljalnica.Message{
		Id:        nextMessageId,
		TopicId:   req.TopicId,
		TopicName: s.storage.GetTopicById(req.TopicId).Name,
		UserId:    req.UserId,
		UserName:  s.storage.GetUserById(req.UserId).Name,
		Text:      req.Text,
		CreatedAt: timestamppb.New(time.Now()),
		Likes:     0,
	}

	// povecamo next message id
	nextMessageId++

	if err := s.storage.AddMessage(message); err != nil {
		return nil, err
	}

	entry := &LogEntry{
		Op:       OpCreateMessage,
		Message:  message,
		Sequence: s.nextSequence(),
	}
	s.log.Add(entry)

	// replikacija + zaklep branja
	//s.mu.RLock()
	next := s.nextNode
	//s.mu.RUnlock()

	if next != nil {
		_, err := next.PostMessage(ctx, req)
		if err != nil {
			fmt.Print("Error posting a message:", err)
			fmt.Printf("CurrentNode: %s", s.nodeId)
		}
	}

	// razposlji lokalnim narocnikom
	go s.broadcastToSubscribers(message, razpravljalnica.OpType_POST)

	log.Printf("postMessage -> %d (%s) at topic %d (%s) by user %d (%s)", message.Id, message.Text, message.TopicId, message.TopicName, message.UserId, message.UserName)
	return message, nil
}

func (s *MessageBoardServer) broadcastToSubscribers(c *razpravljalnica.Message, op razpravljalnica.OpType) {
	topicIDStr := fmt.Sprint(c.TopicId)

	//  razposlji lokalnim narocnikom
	userSubs, ok := s.subs[topicIDStr]
	if !ok {
		// ni narocnikov za to temo
		return
	}

	for _, ch := range userSubs {
		ev := &razpravljalnica.MessageEvent{
			SequenceNumber: s.nextSequence(),
			Op:             op,
			Message: &razpravljalnica.Message{
				Id:        c.Id,
				TopicId:   c.TopicId,
				TopicName: c.TopicName,
				UserId:    c.UserId,
				UserName:  c.UserName,
				Text:      c.Text,
				CreatedAt: c.CreatedAt,
				Likes:     c.Likes,
			},
			EventAt: timestamppb.New(time.Now()),
		}
		select {
		// v primeru da je client offline in je v njegovem kanalu ze 100 sporocil
		// bi tukaj ce ne bi imeli select stavka cakali
		case ch <- ev:
		default:
			// spusti, ce je narocnik pocasen
		}
	}
}

// ------------------------ UpdateMessage -------------------------
func (s *MessageBoardServer) UpdateMessage(ctx context.Context, req *razpravljalnica.UpdateMessageRequest) (*razpravljalnica.Message, error) {
	comment, err := s.storage.GetMessage(req.TopicId, req.MessageId)
	if err != nil {
		return nil, err
	}
	if comment.UserId != req.UserId {
		return nil, errors.New("user not authorized to update this message")
	}

	// dodano za debuging, da vidim kateri je star message pri izpisu, preden se posodobi
	temp := comment.Text

	comment.Text = req.Text
	comment.CreatedAt = timestamppb.New(time.Now())
	if err := s.storage.UpdateMessage(comment); err != nil {
		return nil, err
	}

	entry := &LogEntry{
		Op:       OpUpdateMessage,
		Message:  comment,
		Sequence: s.nextSequence(),
	}
	s.log.Add(entry)

	// replikacija + zaklep branja
	//s.mu.RLock()
	next := s.nextNode
	//s.mu.RUnlock()

	if next != nil {
		_, err := next.UpdateMessage(ctx, req)
		if err != nil {
			fmt.Print("Error updating a message:", err)
			fmt.Printf("CurrentNode: %s", s.nodeId)
		}
	}

	go s.broadcastToSubscribers(comment, razpravljalnica.OpType_UPDATE)

	log.Printf("updateMessage -> %d (%s) at %d (%s) by %d (%s) to \"%s\"", comment.Id, temp, comment.TopicId, comment.TopicName, comment.UserId, comment.UserName, comment.Text)
	return &razpravljalnica.Message{
		Id:        comment.Id,
		TopicId:   comment.TopicId,
		TopicName: comment.TopicName,
		UserId:    comment.UserId,
		UserName:  comment.UserName,
		Text:      comment.Text,
		CreatedAt: comment.CreatedAt,
		Likes:     comment.Likes,
	}, nil
}

// ------------------------ DeleteMessage -------------------------
func (s *MessageBoardServer) DeleteMessage(ctx context.Context, req *razpravljalnica.DeleteMessageRequest) (*emptypb.Empty, error) {
	comment, err := s.storage.GetMessage(req.TopicId, req.MessageId)
	if err != nil {
		return nil, err
	}
	if comment.UserId != req.UserId {
		return nil, errors.New("user not authorized to delete this message")
	}

	temp := comment.Text

	if err := s.storage.DeleteMessage(req.TopicId, req.MessageId, req.UserId); err != nil {
		return nil, err
	}

	entry := &LogEntry{
		Op:       OpDeleteMessage,
		Message:  comment,
		Sequence: s.nextSequence(),
	}
	s.log.Add(entry)

	// replikacija + zaklep branja
	//s.mu.RLock()
	next := s.nextNode
	//s.mu.RUnlock()

	if next != nil {
		_, err := next.DeleteMessage(ctx, req)
		if err != nil {
			fmt.Print("Error deleting a message:", err)
			fmt.Printf("CurrentNode: %s", s.nodeId)
		}
	}

	go s.broadcastToSubscribers(comment, razpravljalnica.OpType_DELETE)

	log.Printf("deleteMessage -> %d (%s) at %d (%s) deleted by %d (%s)", req.MessageId, temp, req.TopicId, comment.TopicName, req.UserId, comment.UserName)
	return &emptypb.Empty{}, nil
}

// ------------------------ LikeMessage ---------------------------
func (s *MessageBoardServer) LikeMessage(ctx context.Context, req *razpravljalnica.LikeMessageRequest) (*razpravljalnica.Message, error) {
	comment, err := s.storage.LikeMessage(req.TopicId, req.MessageId)
	if err != nil {
		return nil, err
	}

	temp := comment.Text

	entry := &LogEntry{
		Op:       OpLikeMessage,
		Message:  comment,
		Sequence: s.nextSequence(),
	}
	s.log.Add(entry)

	// replikacija + zaklep branja
	//s.mu.RLock()
	next := s.nextNode
	//s.mu.RUnlock()

	if next != nil {
		_, err := next.LikeMessage(ctx, req)
		if err != nil {
			fmt.Print("Error liking a message:", err)
			fmt.Printf("CurrentNode: %s", s.nodeId)
		}
	}

	go s.broadcastToSubscribers(comment, razpravljalnica.OpType_LIKE)

	log.Printf("likeMessage -> %d (%s) at %d (%s) liked by %d (%s)", req.MessageId, temp, req.TopicId, comment.TopicName, req.UserId, comment.UserName)
	return &razpravljalnica.Message{
		Id:        comment.Id,
		TopicId:   comment.TopicId,
		TopicName: comment.TopicName,
		UserId:    comment.UserId,
		UserName:  comment.UserName,
		Text:      comment.Text,
		CreatedAt: comment.CreatedAt,
		Likes:     comment.Likes,
	}, nil
}

// ------------------------ ListTopics ----------------------------
func (s *MessageBoardServer) ListTopics(ctx context.Context, _ *emptypb.Empty) (*razpravljalnica.ListTopicsResponse, error) {
	topics := s.storage.ListTopics()
	apiTopics := make([]*razpravljalnica.Topic, 0, len(topics))
	for _, t := range topics {
		apiTopics = append(apiTopics, &razpravljalnica.Topic{
			Id:   t.Id,
			Name: t.Name,
		})
	}

	log.Printf("listTopics -> OK")
	return &razpravljalnica.ListTopicsResponse{Topics: apiTopics}, nil
}

// ------------------------ GetMessages ---------------------------
func (s *MessageBoardServer) GetMessages(ctx context.Context, req *razpravljalnica.GetMessagesRequest) (*razpravljalnica.GetMessagesResponse, error) {
	messages, err := s.storage.GetMessages(req.TopicId, req.FromMessageId, int(req.Limit))
	if err != nil {
		return nil, err
	}

	apiMessages := make([]*razpravljalnica.Message, 0, len(messages))
	for _, m := range messages {
		apiMessages = append(apiMessages, &razpravljalnica.Message{
			Id:        m.Id,
			TopicId:   m.TopicId,
			TopicName: m.TopicName,
			UserId:    m.UserId,
			UserName:  m.UserName,
			Text:      m.Text,
			CreatedAt: m.CreatedAt,
			Likes:     m.Likes,
		})
	}

	log.Printf("getMessages -> at topic %d (%s) from id %d limit %d", req.TopicId, s.storage.GetTopicById(req.TopicId).Name, req.FromMessageId, req.Limit)
	return &razpravljalnica.GetMessagesResponse{Messages: apiMessages}, nil
}

// ------------------------ SubscribeTopic ------------------------
// grpc poskrbi da se vrne grpc.ServerStreamingClient[razpravljalnica.MessageEvent]
func (s *MessageBoardServer) SubscribeTopic(req *razpravljalnica.SubscribeTopicRequest, stream razpravljalnica.MessageBoard_SubscribeTopicServer) error {
	for _, topicId := range req.TopicId {
		// pridobi stara sporocila in vrni error ce topic not found ali ni sporocil v topicu
		msgs, err := s.storage.GetMessages(topicId, req.FromMessageId, 100)
		if err != nil && !strings.Contains(err.Error(), "no messages for topic") {
			return err
		}

		if _, ok := s.subs[fmt.Sprint(topicId)]; !ok {
			// ustvari kanal, ce topic se ni shranjen v subs
			s.subs[fmt.Sprint(topicId)] = make(map[string]chan *razpravljalnica.MessageEvent)
		}
		ch := make(chan *razpravljalnica.MessageEvent, 100)
		// shranis kanal subscriberja v subs
		s.subs[fmt.Sprint(topicId)][fmt.Sprint(req.UserId)] = ch

		// poslji stara sporocila
		for _, m := range msgs {
			ev := &razpravljalnica.MessageEvent{
				SequenceNumber: s.nextSequence(),
				Op:             razpravljalnica.OpType_POST,
				Message: &razpravljalnica.Message{
					Id:        m.Id,
					TopicId:   m.TopicId,
					TopicName: m.TopicName,
					UserId:    m.UserId,
					UserName:  m.UserName,
					Text:      m.Text,
					CreatedAt: m.CreatedAt,
					Likes:     m.Likes,
				},
				EventAt: timestamppb.New(time.Now()),
			}
			if err := stream.Send(ev); err != nil {
				return err
			}
		}

		// posljijaj nova sporocila
		go func(ch chan *razpravljalnica.MessageEvent) {
			for ev := range ch {
				if err := stream.Send(ev); err != nil {
					return
				}
			}
		}(ch)
	}
	// blokiramo, da ostane stream odprt
	select {}
}
