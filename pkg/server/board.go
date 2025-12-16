package server

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	api "github.com/djagodic/razpravljalnica/pkg/api"
	"github.com/djagodic/razpravljalnica/pkg/control"
	"google.golang.org/protobuf/types/known/emptypb"
	"github.com/djagodic/razpravljalnica/pkg/common"
)

type MessageBoardServer struct {
	api.UnimplementedMessageBoardServer

	storage *NodeStorage
	log     *ReplicationLog

	seq int64

	nodeID string
	IsHead bool
	IsTail bool
	nextNode *Node

	// subscription channels: topicID -> userID -> chan *api.MessageEvent
	subs map[string]map[string]chan *api.MessageEvent
}

func NewMessageBoardServer(nodeID string, isHead, isTail bool) *MessageBoardServer {
	return &MessageBoardServer{
		storage: NewNodeStorage(),
		log:     NewReplicationLog(),
		nodeID:  nodeID,
		IsHead:  isHead,
		IsTail:  isTail,
		subs:    make(map[string]map[string]chan *api.MessageEvent),
	}
}

// nextSequence returns monotonic sequence number
func (s *MessageBoardServer) nextSequence() int64 {
	return atomic.AddInt64(&s.seq, 1)
}

// ------------------------ gRPC methods -----------------------------

func (s *MessageBoardServer) CreateUser(ctx context.Context, req *api.CreateUserRequest) (*api.User, error) {
	user := &User{
		ID:   common.GenID("user"),
		Name: req.Name,
	}
	s.storage.AddUser(user)

	// log and replicate
	entry := &LogEntry{
		Op:       OpCreateUser,
		User:     user,
		Sequence: s.nextSequence(),
	}
	s.log.Add(entry)
	if s.IsHead {
		go s.ReplicateEntry(entry)
	}

	return &api.User{
		Id:   user.ID,
		Name: user.Name,
	}, nil
}

func (s *MessageBoardServer) CreateTopic(ctx context.Context, req *api.CreateTopicRequest) (*api.Topic, error) {
	topic := &Topic{
		ID:   common.GenID("topic"),
		Name: req.Name,
	}
	s.storage.AddTopic(topic)

	entry := &LogEntry{
		Op:       OpCreateTopic,
		Topic:    topic,
		Sequence: s.nextSequence(),
	}
	s.log.Add(entry)
	if s.IsHead {
		go s.ReplicateEntry(entry)
	}

	return &api.Topic{
		Id:   topic.ID,
		Name: topic.Name,
	}, nil
}

func (s *MessageBoardServer) PostMessage(ctx context.Context, req *api.PostMessageRequest) (*api.Message, error) {
	comment := &Comment{
		ID:        common.GenID("msg"),
		TopicID:   req.TopicId,
		UserID:    req.UserId,
		Body:      req.Text,
		Timestamp: time.Now(),
		Likes:     0,
	}

	if err := s.storage.AddComment(comment); err != nil {
		return nil, err
	}

	entry := &LogEntry{
		Op:       OpCreateComment,
		Comment:  comment,
		Sequence: s.nextSequence(),
	}
	s.log.Add(entry)
	if s.IsHead {
		go s.ReplicateEntry(entry)
	}

	// broadcast to local subscribers
	go s.broadcastToSubscribers(comment, api.OpType_OP_POST)

	return &api.Message{
		Id:        comment.ID,
		TopicId:   comment.TopicID,
		UserId:    comment.UserID,
		Text:      comment.Body,
		CreatedAt: timestamppbNow(comment.Timestamp),
		Likes:     comment.Likes,
	}, nil
}

// helper
func (s *MessageBoardServer) broadcastToSubscribers(c *Comment, op api.OpType) {
	for _, userSubs := range s.subs {
		for _, ch := range userSubs {
			ev := &api.MessageEvent{
				SequenceNumber: s.nextSequence(),
				Op:             op,
				Message: &api.Message{
					Id:        c.ID,
					TopicId:   c.TopicID,
					UserId:    c.UserID,
					Text:      c.Body,
					CreatedAt: timestamppbNow(c.Timestamp),
					Likes:     c.Likes,
				},
				EventAt: timestamppbNow(time.Now()),
			}
			select {
			case ch <- ev:
			default:
				// drop if subscriber is slow
			}
		}
	}
}

// Helper to convert time.Time -> protobuf Timestamp
func timestamppbNow(t time.Time) *api.Timestamp {
	return &api.Timestamp{
		Seconds: t.Unix(),
		Nanos:   int32(t.Nanosecond()),
	}
}


// ------------------------ UpdateMessage -------------------------
func (s *MessageBoardServer) UpdateMessage(ctx context.Context, req *api.UpdateMessageRequest) (*api.Message, error) {
	comment, err := s.storage.GetComment(req.TopicId, req.MessageId)
	if err != nil {
		return nil, err
	}
	if comment.UserID != req.UserId {
		return nil, errors.New("user not authorized to update this message")
	}

	comment.Body = req.Text
	comment.Timestamp = time.Now()
	if err := s.storage.UpdateComment(comment); err != nil {
		return nil, err
	}

	entry := &LogEntry{
		Op:       OpUpdateComment,
		Comment:  comment,
		Sequence: s.nextSequence(),
	}
	s.log.Add(entry)
	if s.IsHead {
		go s.ReplicateEntry(entry)
	}

	go s.broadcastToSubscribers(comment, api.OpType_OP_UPDATE)

	return &api.Message{
		Id:        comment.ID,
		TopicId:   comment.TopicID,
		UserId:    comment.UserID,
		Text:      comment.Body,
		CreatedAt: timestamppbNow(comment.Timestamp),
		Likes:     comment.Likes,
	}, nil
}

// ------------------------ DeleteMessage -------------------------
func (s *MessageBoardServer) DeleteMessage(ctx context.Context, req *api.DeleteMessageRequest) (*emptypb.Empty, error) {
	comment, err := s.storage.GetComment(req.TopicId, req.MessageId)
	if err != nil {
		return nil, err
	}
	if comment.UserID != req.UserId {
		return nil, errors.New("user not authorized to delete this message")
	}

	if err := s.storage.DeleteComment(req.TopicId, req.MessageId, req.UserId); err != nil {
		return nil, err
	}

	entry := &LogEntry{
		Op:       OpDeleteComment,
		Comment:  comment,
		Sequence: s.nextSequence(),
	}
	s.log.Add(entry)
	if s.IsHead {
		go s.ReplicateEntry(entry)
	}

	go s.broadcastToSubscribers(comment, api.OpType_OP_DELETE)

	return &emptypb.Empty{}, nil
}

// ------------------------ LikeMessage ---------------------------
func (s *MessageBoardServer) LikeMessage(ctx context.Context, req *api.LikeMessageRequest) (*api.Message, error) {
	comment, err := s.storage.LikeComment(req.TopicId, req.MessageId)
	if err != nil {
		return nil, err
	}

	entry := &LogEntry{
		Op:       OpLikeComment,
		Comment:  comment,
		Sequence: s.nextSequence(),
	}
	s.log.Add(entry)
	if s.IsHead {
		go s.ReplicateEntry(entry)
	}

	go s.broadcastToSubscribers(comment, api.OpType_OP_LIKE)

	return &api.Message{
		Id:        comment.ID,
		TopicId:   comment.TopicID,
		UserId:    comment.UserID,
		Text:      comment.Body,
		CreatedAt: timestamppbNow(comment.Timestamp),
		Likes:     comment.Likes,
	}, nil
}

// ------------------------ ListTopics ----------------------------
func (s *MessageBoardServer) ListTopics(ctx context.Context, _ *emptypb.Empty) (*api.ListTopicsResponse, error) {
	topics := s.storage.ListTopics()
	apiTopics := make([]*api.Topic, 0, len(topics))
	for _, t := range topics {
		apiTopics = append(apiTopics, &api.Topic{
			Id:   t.ID,
			Name: t.Name,
		})
	}
	return &api.ListTopicsResponse{Topics: apiTopics}, nil
}

// ------------------------ GetMessages ---------------------------
func (s *MessageBoardServer) GetMessages(ctx context.Context, req *api.GetMessagesRequest) (*api.GetMessagesResponse, error) {
	messages := s.storage.GetMessages(req.TopicId, req.FromMessageId, int(req.Limit))
	apiMessages := make([]*api.Message, 0, len(messages))
	for _, m := range messages {
		apiMessages = append(apiMessages, &api.Message{
			Id:        m.ID,
			TopicId:   m.TopicID,
			UserId:    m.UserID,
			Text:      m.Body,
			CreatedAt: timestamppbNow(m.Timestamp),
			Likes:     m.Likes,
		})
	}
	return &api.GetMessagesResponse{Messages: apiMessages}, nil
}

// ------------------------ SubscribeTopic ------------------------
func (s *MessageBoardServer) SubscribeTopic(req *api.SubscribeTopicRequest, stream api.MessageBoard_SubscribeTopicServer) error {
	for _, topicID := range req.TopicId {
		if _, ok := s.subs[fmt.Sprint(topicID)]; !ok {
			s.subs[fmt.Sprint(topicID)] = make(map[string]chan *api.MessageEvent)
		}
		ch := make(chan *api.MessageEvent, 100)
		s.subs[fmt.Sprint(topicID)][fmt.Sprint(req.UserId)] = ch

		// send historical messages
		msgs := s.storage.GetMessages(topicID, req.FromMessageId, 100)
		for _, m := range msgs {
			ev := &api.MessageEvent{
				SequenceNumber: s.nextSequence(),
				Op:             api.OpType_OP_POST,
				Message: &api.Message{
					Id:        m.ID,
					TopicId:   m.TopicID,
					UserId:    m.UserID,
					Text:      m.Body,
					CreatedAt: timestamppbNow(m.Timestamp),
					Likes:     m.Likes,
				},
				EventAt: timestamppbNow(time.Now()),
			}
			if err := stream.Send(ev); err != nil {
				return err
			}
		}

		// stream new messages
		go func(ch chan *api.MessageEvent) {
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

