package server

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	razpravljalnica "github.com/djagodic/razpravljalnica/pkg/api/razpravljalnica"
	"github.com/djagodic/razpravljalnica/pkg/common"
	"google.golang.org/protobuf/types/known/emptypb"
)

type MessageBoardServer struct {
	razpravljalnica.UnimplementedMessageBoardServer

	storage *NodeStorage
	log     *ReplicationLog

	seq int64

	nodeID   string
	IsHead   bool
	IsTail   bool
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
		subs:    make(map[string]map[string]chan *razpravljalnica.MessageEvent),
	}
}

// nextSequence returns monotonic sequence number
func (s *MessageBoardServer) nextSequence() int64 {
	return atomic.AddInt64(&s.seq, 1)
}

// ------------------------ gRPC methods -----------------------------

func (s *MessageBoardServer) CreateUser(ctx context.Context, req *razpravljalnica.CreateUserRequest) (*razpravljalnica.User, error) {
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

	return &razpravljalnica.User{
		Id:   user.ID,
		Name: user.Name,
	}, nil
}

func (s *MessageBoardServer) CreateTopic(ctx context.Context, req *razpravljalnica.CreateTopicRequest) (*razpravljalnica.Topic, error) {
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

	return &razpravljalnica.Topic{
		Id:   topic.ID,
		Name: topic.Name,
	}, nil
}

func (s *MessageBoardServer) PostMessage(ctx context.Context, req *razpravljalnica.PostMessageRequest) (*razpravljalnica.Message, error) {
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
	go s.broadcastToSubscribers(comment, razpravljalnica.OpType_OP_POST)

	return &razpravljalnica.Message{
		Id:        comment.ID,
		TopicId:   comment.TopicID,
		UserId:    comment.UserID,
		Text:      comment.Body,
		CreatedAt: timestamppbNow(comment.Timestamp),
		Likes:     comment.Likes,
	}, nil
}

// helper
func (s *MessageBoardServer) broadcastToSubscribers(c *Comment, op razpravljalnica.OpType) {
	for _, userSubs := range s.subs {
		for _, ch := range userSubs {
			ev := &razpravljalnica.MessageEvent{
				SequenceNumber: s.nextSequence(),
				Op:             op,
				Message: &razpravljalnica.Message{
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
func timestamppbNow(t time.Time) *razpravljalnica.Timestamp {
	return &razpravljalnica.Timestamp{
		Seconds: t.Unix(),
		Nanos:   int32(t.Nanosecond()),
	}
}

// ------------------------ UpdateMessage -------------------------
func (s *MessageBoardServer) UpdateMessage(ctx context.Context, req *api.UpdateMessageRequest) (*razpravljalnica.Message, error) {
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

	go s.broadcastToSubscribers(comment, razpravljalnica.OpType_OP_UPDATE)

	return &razpravljalnica.Message{
		Id:        comment.ID,
		TopicId:   comment.TopicID,
		UserId:    comment.UserID,
		Text:      comment.Body,
		CreatedAt: timestamppbNow(comment.Timestamp),
		Likes:     comment.Likes,
	}, nil
}

// ------------------------ DeleteMessage -------------------------
func (s *MessageBoardServer) DeleteMessage(ctx context.Context, req *razpravljalnica.DeleteMessageRequest) (*emptypb.Empty, error) {
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

	go s.broadcastToSubscribers(comment, razpravljalnica.OpType_OP_DELETE)

	return &emptypb.Empty{}, nil
}

// ------------------------ LikeMessage ---------------------------
func (s *MessageBoardServer) LikeMessage(ctx context.Context, req *razpravljalnica.LikeMessageRequest) (*razpravljalnica.Message, error) {
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

	go s.broadcastToSubscribers(comment, razpravljalnica.OpType_OP_LIKE)

	return &razpravljalnica.Message{
		Id:        comment.ID,
		TopicId:   comment.TopicID,
		UserId:    comment.UserID,
		Text:      comment.Body,
		CreatedAt: timestamppbNow(comment.Timestamp),
		Likes:     comment.Likes,
	}, nil
}

// ------------------------ ListTopics ----------------------------
func (s *MessageBoardServer) ListTopics(ctx context.Context, _ *emptypb.Empty) (*razpravljalnica.ListTopicsResponse, error) {
	topics := s.storage.ListTopics()
	apiTopics := make([]*razpravljalnica.Topic, 0, len(topics))
	for _, t := range topics {
		apiTopics = append(apiTopics, &razpravljalnica.Topic{
			Id:   t.ID,
			Name: t.Name,
		})
	}
	return &razpravljalnica.ListTopicsResponse{Topics: apiTopics}, nil
}

// ------------------------ GetMessages ---------------------------
func (s *MessageBoardServer) GetMessages(ctx context.Context, req *razpravljalnica.GetMessagesRequest) (*razpravljalnica.GetMessagesResponse, error) {
	messages := s.storage.GetMessages(req.TopicId, req.FromMessageId, int(req.Limit))
	apiMessages := make([]*razpravljalnica.Message, 0, len(messages))
	for _, m := range messages {
		apiMessages = append(apiMessages, &razpravljalnica.Message{
			Id:        m.ID,
			TopicId:   m.TopicID,
			UserId:    m.UserID,
			Text:      m.Body,
			CreatedAt: timestamppbNow(m.Timestamp),
			Likes:     m.Likes,
		})
	}
	return &razpravljalnica.GetMessagesResponse{Messages: apiMessages}, nil
}

// ------------------------ SubscribeTopic ------------------------
func (s *MessageBoardServer) SubscribeTopic(req *razpravljalnica.SubscribeTopicRequest, stream razpravljalnica.MessageBoard_SubscribeTopicServer) error {
	for _, topicID := range req.TopicId {
		if _, ok := s.subs[fmt.Sprint(topicID)]; !ok {
			s.subs[fmt.Sprint(topicID)] = make(map[string]chan *razpravljalnica.MessageEvent)
		}
		ch := make(chan *razpravljalnica.MessageEvent, 100)
		s.subs[fmt.Sprint(topicID)][fmt.Sprint(req.UserId)] = ch

		// send historical messages
		msgs := s.storage.GetMessages(topicID, req.FromMessageId, 100)
		for _, m := range msgs {
			ev := &razpravljalnica.MessageEvent{
				SequenceNumber: s.nextSequence(),
				Op:             razpravljalnica.OpType_OP_POST,
				Message: &razpravljalnica.Message{
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
