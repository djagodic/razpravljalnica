package server

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	razpravljalnica "github.com/djagodic/razpravljalnica/pkg/api/razpravljalnica"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var nextUserId int64
var nextTopicId int64
var nextMessageId int64

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

	return topic, nil
}

func (s *MessageBoardServer) PostMessage(ctx context.Context, req *razpravljalnica.PostMessageRequest) (*razpravljalnica.Message, error) {
	message := &razpravljalnica.Message{
		Id:        nextMessageId,
		TopicId:   req.TopicId,
		UserId:    req.UserId,
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
	go s.broadcastToSubscribers(message, razpravljalnica.OpType_OP_POST)

	return message, nil
}

// helper
func (s *MessageBoardServer) broadcastToSubscribers(c *razpravljalnica.Message, op razpravljalnica.OpType) {
	for _, userSubs := range s.subs {
		for _, ch := range userSubs {
			ev := &razpravljalnica.MessageEvent{
				SequenceNumber: s.nextSequence(),
				Op:             op,
				Message: &razpravljalnica.Message{
					Id:        c.Id,
					TopicId:   c.TopicId,
					UserId:    c.UserId,
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

	go s.broadcastToSubscribers(comment, razpravljalnica.OpType_OP_UPDATE)

	return &razpravljalnica.Message{
		Id:        comment.Id,
		TopicId:   comment.TopicId,
		UserId:    comment.UserId,
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

	go s.broadcastToSubscribers(comment, razpravljalnica.OpType_OP_DELETE)

	return &emptypb.Empty{}, nil
}

// ------------------------ LikeMessage ---------------------------
func (s *MessageBoardServer) LikeMessage(ctx context.Context, req *razpravljalnica.LikeMessageRequest) (*razpravljalnica.Message, error) {
	comment, err := s.storage.LikeMessage(req.TopicId, req.MessageId)
	if err != nil {
		return nil, err
	}

	entry := &LogEntry{
		Op:       OpLikeMessage,
		Message:  comment,
		Sequence: s.nextSequence(),
	}
	s.log.Add(entry)
	if s.IsHead {
		//go s.ReplicateEntry(entry)
	}

	go s.broadcastToSubscribers(comment, razpravljalnica.OpType_OP_LIKE)

	return &razpravljalnica.Message{
		Id:        comment.Id,
		TopicId:   comment.TopicId,
		UserId:    comment.UserId,
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
			UserId:    m.UserId,
			Text:      m.Text,
			CreatedAt: m.CreatedAt,
			Likes:     m.Likes,
		})
	}
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
				Op:             razpravljalnica.OpType_OP_POST,
				Message: &razpravljalnica.Message{
					Id:        m.Id,
					TopicId:   m.TopicId,
					UserId:    m.UserId,
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
