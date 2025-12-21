package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	razpravljalnica "github.com/djagodic/razpravljalnica/pkg/api/razpravljalnica"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

//naj gredo od 1 naprej, lazje uporabnikov, patch da subscribe vrne tudi prvi message (ker z 0 ni hotlo)
var nextUserId int64 = 1
var nextTopicId int64 = 1
var nextMessageId int64 = 1

type MessageBoardServer struct {
	razpravljalnica.UnimplementedMessageBoardServer

	storage *NodeStorage
	log     *ReplicationLog

	seq int64

	nodeId   string
	IsHead   bool
	IsTail   bool
	NextNode *Node

	// subscription channels: topicId -> userId -> chan *api.MessageEvent
	subs map[string]map[string]chan *razpravljalnica.MessageEvent
}

func NewMessageBoardServer(nodeId string, isHead, isTail bool) *MessageBoardServer {
	return &MessageBoardServer{
		storage: NewNodeStorage(),
		log:     NewReplicationLog(),
		nodeId:  nodeId,
		IsHead:  isHead,
		IsTail:  isTail,
		subs:    make(map[string]map[string]chan *razpravljalnica.MessageEvent),
	}
}

// nextSequence returns monotonic sequence number
func (s *MessageBoardServer) nextSequence() int64 {
	return atomic.AddInt64(&s.seq, 1)
}

// ------------------------ gRPC methods -----------------------------
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

	//povecamo next user Id
	//TODO zamenjaj s cim bolj robustnim
	nextUserId++

	// log and replicate
	entry := &LogEntry{
		Op:       OpCreateUser,
		User:     user,
		Sequence: s.nextSequence(),
	}
	s.log.Add(entry)
	if s.IsHead {
		//go s.ReplicateEntry(entry)
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

	//povecamo next topic id
	nextTopicId++

	entry := &LogEntry{
		Op:       OpCreateTopic,
		Topic:    topic,
		Sequence: s.nextSequence(),
	}
	s.log.Add(entry)
	if s.IsHead {
		//go s.ReplicateEntry(entry)
	}

	log.Printf("createTopic -> %d (%s)", topic.Id, topic.Name)
	return topic, nil
}

func (s *MessageBoardServer) PostMessage(ctx context.Context, req *razpravljalnica.PostMessageRequest) (*razpravljalnica.Message, error) {
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

	//povecamo next message id
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
	if s.IsHead {
		//go s.ReplicateEntry(entry)
	}

	// broadcast to local subscribers
	go s.broadcastToSubscribers(message, razpravljalnica.OpType_POST)

	log.Printf("postMessage -> %d (%s) at topic %d (%s) by user %d (%s)", message.Id, message.Text, message.TopicId,message.TopicName, message.UserId, message.UserName)
	return message, nil
}

// helper
// func (s *MessageBoardServer) broadcastToSubscribers(c *razpravljalnica.Message, op razpravljalnica.OpType) {
// 	for _, userSubs := range s.subs {
// 		for _, ch := range userSubs {
// 			ev := &razpravljalnica.MessageEvent{
// 				SequenceNumber: s.nextSequence(),
// 				Op:             op,
// 				Message: &razpravljalnica.Message{
// 					Id:        c.Id,
// 					TopicId:   c.TopicId,
// 					UserId:    c.UserId,
// 					UserName:  c.UserName,
// 					Text:      c.Text,
// 					CreatedAt: c.CreatedAt,
// 					Likes:     c.Likes,
// 				},
// 				EventAt: timestamppb.New(time.Now()),
// 			}
// 			select {
// 			case ch <- ev:
// 			default:
// 				// drop if subscriber is slow
// 			}
// 		}
// 	}
// }

func (s *MessageBoardServer) broadcastToSubscribers(c *razpravljalnica.Message, op razpravljalnica.OpType) {
	topicIDStr := fmt.Sprint(c.TopicId)

	// get subscribers only for this topic
	userSubs, ok := s.subs[topicIDStr]
	if !ok {
		// no subscribers for this topic
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
		case ch <- ev:
		default:
			// drop if subscriber is slow
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

	//added for debuging, da vidim kateri je star message pri izpisu, preden se posodobi
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
	if s.IsHead {
		//go s.ReplicateEntry(entry)
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
	if s.IsHead {
		//go s.ReplicateEntry(entry)
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
	if s.IsHead {
		//go s.ReplicateEntry(entry)
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
func (s *MessageBoardServer) SubscribeTopic(req *razpravljalnica.SubscribeTopicRequest, stream razpravljalnica.MessageBoard_SubscribeTopicServer) error {
	for _, topicId := range req.TopicId {
		if _, ok := s.subs[fmt.Sprint(topicId)]; !ok {
			s.subs[fmt.Sprint(topicId)] = make(map[string]chan *razpravljalnica.MessageEvent)
		}
		ch := make(chan *razpravljalnica.MessageEvent, 100)
		s.subs[fmt.Sprint(topicId)][fmt.Sprint(req.UserId)] = ch

		// send historical messages
		msgs, err := s.storage.GetMessages(topicId, req.FromMessageId, 100)
		if err != nil {
			return err
		}
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

		// stream new messages
		go func(ch chan *razpravljalnica.MessageEvent) {
			for ev := range ch {
				if err := stream.Send(ev); err != nil {
					return
				}
			}
		}(ch)
	}
	// block to keep the stream open
	select {}
}

