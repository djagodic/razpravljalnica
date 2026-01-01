package server

import (
	"errors"
	"sort"
	"sync"

	razpravljalnica "github.com/djagodic/razpravljalnica/pkg/api/razpravljalnica"
)

// NodeStorage hrani vse podatke v pomnilniku za posamezno vozlisce
type NodeStorage struct {
	mu sync.RWMutex

	users    map[int64]*razpravljalnica.User
	topics   map[int64]*razpravljalnica.Topic
	comments map[int64]map[int64]*razpravljalnica.Message // topicId -> commentId -> Message
}

// NewNodeStorage ustvari prazno shrambo
func NewNodeStorage() *NodeStorage {
	return &NodeStorage{
		users:    make(map[int64]*razpravljalnica.User),
		topics:   make(map[int64]*razpravljalnica.Topic),
		comments: make(map[int64]map[int64]*razpravljalnica.Message),
	}
}

// pridobi uporabnika po imenu
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

// dodano zato, da lahko dobivam imena iz userId, ki jih imamo v message-ih
func (ns *NodeStorage) GetUserById(id int64) *razpravljalnica.User {
	ns.mu.RLock()
	defer ns.mu.RUnlock()
	return ns.users[id] // assuming ns.users map[int64]*User
}

// dodano zato, da lahko dobivam topice iz topicId, ki jih imamo v message-ih
func (ns *NodeStorage) GetTopicById(id int64) *razpravljalnica.Topic {
	ns.mu.RLock()
	defer ns.mu.RUnlock()
	return ns.topics[id] // assuming ns.users map[int64]*User
}

// AddUser doda novega uporabnika
func (s *NodeStorage) AddUser(u *razpravljalnica.User) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.users[u.Id] = u
}

// AddTopic doda novo temo
func (s *NodeStorage) AddTopic(t *razpravljalnica.Topic) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.topics[t.Id] = t
	if _, ok := s.comments[t.Id]; !ok {
		s.comments[t.Id] = make(map[int64]*razpravljalnica.Message)
	}
}

// ListTopics vrne vse teme
func (s *NodeStorage) ListTopics() []*razpravljalnica.Topic {
	s.mu.RLock()
	defer s.mu.RUnlock()

	topics := make([]*razpravljalnica.Topic, 0, len(s.topics))
	for _, topic := range s.topics {
		topics = append(topics, topic)
	}

	return topics
}

// AddMessage doda sporocilo
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

	// preveri ali tema obstaja
	if _, ok := s.topics[topicId]; !ok {
		return nil, errors.New("topic not found")
	}

	// preveri ali obstajajo sporocila za temo
	commentsByTopic, ok := s.comments[topicId]
	if !ok {
		return nil, errors.New("no messages for topic")
	}

	// pridobi sporocilo
	msg, ok := commentsByTopic[messageId]
	if !ok {
		return nil, errors.New("message not found")
	}

	return msg, nil
}

// GetMessages vrne sporocila za temo, zacetek po fromMessageId,
// omejeno na najvec limit sporocil
func (s *NodeStorage) GetMessages(topicId int64, fromMessageId int64, limit int) ([]*razpravljalnica.Message, error) {

	s.mu.RLock()
	defer s.mu.RUnlock()

	// preveri veljavnost teme
	if _, ok := s.topics[topicId]; !ok {
		return nil, errors.New("topic not found")
	}

	commentsByTopic, ok := s.comments[topicId]
	if !ok || len(commentsByTopic) == 0 {
		return nil, errors.New("no messages for topic")
	}

	// zberi ustrezna sporocila
	messages := make([]*razpravljalnica.Message, 0, len(commentsByTopic))
	for _, msg := range commentsByTopic {
		if msg.Id >= fromMessageId {
			messages = append(messages, msg)
		}
	}

	if len(messages) == 0 {
		return []*razpravljalnica.Message{}, nil
	}

	// zagotovi determinicen vrstni red
	sort.Slice(messages, func(i, j int) bool {
		return messages[i].Id < messages[j].Id
	})

	// uporabi omejitev
	if limit > 0 && len(messages) > limit {
		messages = messages[:limit]
	}

	return messages, nil
}

// UpdateMessage posodobi sporocilo (samo avtor)
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

// DeleteMessage izbrise sporocilo (samo avtor)
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

// LikeMessage poveca stevilo like-ov (vseckov i guess)
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

// dodane funkcije za podporo snapshota -> kopiranje baze na novi tail
func (s *NodeStorage) ListAllMessages() []*razpravljalnica.Message {
    s.mu.RLock()
    defer s.mu.RUnlock()

    res := []*razpravljalnica.Message{}
    for _, topicMsgs := range s.comments {
        for _, m := range topicMsgs {
            res = append(res, m)
        }
    }
    return res
}

func (s *NodeStorage) ListUsers() []*razpravljalnica.User {
    s.mu.RLock()
    defer s.mu.RUnlock()

    res := make([]*razpravljalnica.User, 0, len(s.users))
    for _, u := range s.users {
        res = append(res, u)
    }
    return res
}

// funkcija ki zbrise celo bazo -> uporabi se pred namestitivijo snapshota na novem tailu (pomoje nepotrebno)
func (s *NodeStorage) Reset() {
    s.mu.Lock()
    defer s.mu.Unlock()

    s.users = make(map[int64]*razpravljalnica.User)
    s.topics = make(map[int64]*razpravljalnica.Topic)
    s.comments = make(map[int64]map[int64]*razpravljalnica.Message)
}
