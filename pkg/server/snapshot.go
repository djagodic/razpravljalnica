package server

// Snapshot stores full state for transfer to new node
type Snapshot struct {
	Users    []*User
	Topics   []*Topic
	Comments []*Comment
}

// BuildSnapshot builds snapshot from storage
func BuildSnapshot(storage *NodeStorage) *Snapshot {
	storage.mu.RLock()
	defer storage.mu.RUnlock()

	users := make([]*User, 0, len(storage.users))
	for _, u := range storage.users {
		users = append(users, u)
	}

	topics := make([]*Topic, 0, len(storage.topics))
	for _, t := range storage.topics {
		topics = append(topics, t)
	}

	comments := []*Comment{}
	for _, topicComments := range storage.comments {
		for _, c := range topicComments {
			comments = append(comments, c)
		}
	}

	return &Snapshot{
		Users:    users,
		Topics:   topics,
		Comments: comments,
	}
}

// ApplySnapshot applies snapshot to storage
func ApplySnapshot(storage *NodeStorage, snapshot *Snapshot) {
	storage.mu.Lock()
	defer storage.mu.Unlock()
	storage.users = make(map[string]*User)
	storage.topics = make(map[string]*Topic)
	storage.comments = make(map[string]map[string]*Comment)

	for _, u := range snapshot.Users {
		storage.users[u.ID] = u
	}

	for _, t := range snapshot.Topics {
		storage.topics[t.ID] = t
		storage.comments[t.ID] = make(map[string]*Comment)
	}

	for _, c := range snapshot.Comments {
		if _, ok := storage.comments[c.TopicID]; !ok {
			storage.comments[c.TopicID] = make(map[string]*Comment)
		}
		storage.comments[c.TopicID][c.ID] = c
	}
}
