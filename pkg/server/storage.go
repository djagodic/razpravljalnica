package server

import (
	"errors"
	"sort"
	"sync"

	razpravljalnica "github.com/djagodic/razpravljalnica/pkg/api/razpravljalnica"
)

// NodeStorage holds all in-memory data per node
type NodeStorage struct {
	mu sync.RWMutex

	users    map[int64]*razpravljalnica.User
	topics   map[int64]*razpravljalnica.Topic
	comments map[int64]map[int64]*razpravljalnica.Message // topicId -> commentId -> Message
}

// NewNodeStorage creates empty storage
func NewNodeStorage() *NodeStorage {
	return &NodeStorage{
		users:    make(map[int64]*razpravljalnica.User),
		topics:   make(map[int64]*razpravljalnica.Topic),
		comments: make(map[int64]map[int64]*razpravljalnica.Message),
	}
}

// get user by name
func (s *NodeStorage) GetUserByName(name string) *razpravljalnica.User {
	s.mu.RLock()
	defer s.mu.RUnlock()

	//TODO mogoce malo pocasna implementacija ker moras cez vse?
	for _, u := range s.users {
		if u.Name == name {
			return &razpravljalnica.User{
				Id:   u.Id,
				Name: u.Name,
			}
		}
	}

	return nil
}

//dodano zato, da lahko dobivam imena iz userId, ki jih imamo v message-ih
func (ns *NodeStorage) GetUserById(id int64) *razpravljalnica.User {
	ns.mu.RLock()
	defer ns.mu.RUnlock()
	return ns.users[id] // assuming ns.users map[int64]*User
}


// AddUser adds a new user
func (s *NodeStorage) AddUser(u *razpravljalnica.User) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.users[u.Id] = u
}

// AddTopic adds a new topic
func (s *NodeStorage) AddTopic(t *razpravljalnica.Topic) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.topics[t.Id] = t
	if _, ok := s.comments[t.Id]; !ok {
		s.comments[t.Id] = make(map[int64]*razpravljalnica.Message)
	}
}

// ListTopics returns all topics
func (s *NodeStorage) ListTopics() []*razpravljalnica.Topic {
	s.mu.RLock()
	defer s.mu.RUnlock()

	topics := make([]*razpravljalnica.Topic, 0, len(s.topics))
	for _, topic := range s.topics {
		topics = append(topics, topic)
	}

	return topics
}

// AddMessage adds a comment
func (s *NodeStorage) AddMessage(c *razpravljalnica.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.topics[c.TopicId]; !ok {
		return errors.New("topic not found")
	}
	s.comments[c.TopicId][c.Id] = c
	return nil
}

func (s *NodeStorage) GetMessage(topicId, messageId int64) (*razpravljalnica.Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check if topic exists
	if _, ok := s.topics[topicId]; !ok {
		return nil, errors.New("topic not found")
	}

	// Check if messages exist for the topic
	commentsByTopic, ok := s.comments[topicId]
	if !ok {
		return nil, errors.New("no messages for topic")
	}

	// Retrieve message
	msg, ok := commentsByTopic[messageId]
	if !ok {
		return nil, errors.New("message not found")
	}

	return msg, nil
}

// GetMessages returns messages for a topic starting after fromMessageId,
// limited to at most limit messages.
func (s *NodeStorage) GetMessages(
	topicId int64,
	fromMessageId int64,
	limit int,
) ([]*razpravljalnica.Message, error) {

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Validate topic
	if _, ok := s.topics[topicId]; !ok {
		return nil, errors.New("topic not found")
	}

	commentsByTopic, ok := s.comments[topicId]
	if !ok || len(commentsByTopic) == 0 {
		return nil, errors.New("no messages for topic")
	}

	// Collect eligible messages
	messages := make([]*razpravljalnica.Message, 0, len(commentsByTopic))
	for _, msg := range commentsByTopic {
		if msg.Id > fromMessageId {
			messages = append(messages, msg)
		}
	}

	if len(messages) == 0 {
		return []*razpravljalnica.Message{}, nil
	}

	// Ensure deterministic order
	sort.Slice(messages, func(i, j int) bool {
		return messages[i].Id < messages[j].Id
	})

	// Apply limit
	if limit > 0 && len(messages) > limit {
		messages = messages[:limit]
	}

	return messages, nil
}

// UpdateMessage updates a comment (only by author)
func (s *NodeStorage) UpdateMessage(c *razpravljalnica.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	topicMessage, ok := s.comments[c.TopicId]
	if !ok {
		return errors.New("topic not found")
	}
	existing, ok := topicMessage[c.Id]
	if !ok {
		return errors.New("comment not found")
	}
	if existing.UserId != c.UserId {
		return errors.New("not author")
	}
	existing.Text = c.Text
	return nil
}

// DeleteMessage deletes a comment (only by author)
func (s *NodeStorage) DeleteMessage(topicId, commentId, userId int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	topicMessage, ok := s.comments[topicId]
	if !ok {
		return errors.New("topic not found")
	}
	existing, ok := topicMessage[commentId]
	if !ok {
		return errors.New("comment not found")
	}
	if existing.UserId != userId {
		return errors.New("not author")
	}
	delete(topicMessage, commentId)
	return nil
}

// LikeMessage increments likes
func (s *NodeStorage) LikeMessage(topicId, commentId int64) (*razpravljalnica.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	topicMessage, ok := s.comments[topicId]
	if !ok {
		return nil, errors.New("topic not found")
	}
	c, ok := topicMessage[commentId]
	if !ok {
		return nil, errors.New("comment not found")
	}
	c.Likes++
	return c, nil
}
