package server

import (
	"errors"
	"sync"
)

// NodeStorage holds all in-memory data per node
type NodeStorage struct {
	mu sync.RWMutex

	users    map[string]*User
	topics   map[string]*Topic
	comments map[string]map[string]*Comment // topicID -> commentID -> Comment
}

// NewNodeStorage creates empty storage
func NewNodeStorage() *NodeStorage {
	return &NodeStorage{
		users:    make(map[string]*User),
		topics:   make(map[string]*Topic),
		comments: make(map[string]map[string]*Comment),
	}
}

// AddUser adds a new user
func (s *NodeStorage) AddUser(u *User) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.users[u.ID] = u
}

// AddTopic adds a new topic
func (s *NodeStorage) AddTopic(t *Topic) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.topics[t.ID] = t
	if _, ok := s.comments[t.ID]; !ok {
		s.comments[t.ID] = make(map[string]*Comment)
	}
}

// AddComment adds a comment
func (s *NodeStorage) AddComment(c *Comment) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.topics[c.TopicID]; !ok {
		return errors.New("topic not found")
	}
	s.comments[c.TopicID][c.ID] = c
	return nil
}

// UpdateComment updates a comment (only by author)
func (s *NodeStorage) UpdateComment(c *Comment) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	topicComments, ok := s.comments[c.TopicID]
	if !ok {
		return errors.New("topic not found")
	}
	existing, ok := topicComments[c.ID]
	if !ok {
		return errors.New("comment not found")
	}
	if existing.UserID != c.UserID {
		return errors.New("not author")
	}
	existing.Body = c.Body
	return nil
}

// DeleteComment deletes a comment (only by author)
func (s *NodeStorage) DeleteComment(topicID, commentID, userID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	topicComments, ok := s.comments[topicID]
	if !ok {
		return errors.New("topic not found")
	}
	existing, ok := topicComments[commentID]
	if !ok {
		return errors.New("comment not found")
	}
	if existing.UserID != userID {
		return errors.New("not author")
	}
	delete(topicComments, commentID)
	return nil
}

// LikeComment increments likes
func (s *NodeStorage) LikeComment(topicID, commentID string) (*Comment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	topicComments, ok := s.comments[topicID]
	if !ok {
		return nil, errors.New("topic not found")
	}
	c, ok := topicComments[commentID]
	if !ok {
		return nil, errors.New("comment not found")
	}
	c.Likes++
	return c, nil
}
