package server

import (
	"sync"

	razpravljalnica "github.com/djagodic/razpravljalnica2/pkg/api/razpravljalnica"
)

// OperationType defines replication operation
type OperationType int

const (
	OpCreateUser OperationType = iota
	OpCreateTopic
	OpCreateMessage
	OpUpdateMessage
	OpDeleteMessage
	OpLikeMessage
)

// LogEntry hrani poadtke za replikacijo
type LogEntry struct {
	Op       OperationType
	User     *razpravljalnica.User
	Topic    *razpravljalnica.Topic
	Message  *razpravljalnica.Message
	Sequence int64
}

// ReplicationLog hrani vse uncommitted evente
type ReplicationLog struct {
	mu      sync.Mutex
	entries []*LogEntry
}

func NewReplicationLog() *ReplicationLog {
	return &ReplicationLog{
		entries: []*LogEntry{},
	}
}

func (l *ReplicationLog) Add(entry *LogEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, entry)
}

func (l *ReplicationLog) GetAll() []*LogEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	copied := make([]*LogEntry, len(l.entries))
	copy(copied, l.entries)
	return copied
}

func (l *ReplicationLog) Clear() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = []*LogEntry{}
}