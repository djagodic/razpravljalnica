package server

import (
	"sync"
)

// OperationType defines replication operation
type OperationType int

const (
	OpCreateUser OperationType = iota
	OpCreateTopic
	OpCreateComment
	OpUpdateComment
	OpDeleteComment
	OpLikeComment
)

// LogEntry stores operation for replication
type LogEntry struct {
	Op       OperationType
	User     *User
	Topic    *Topic
	Comment  *Comment
	Sequence int64
}

// ReplicationLog stores all uncommitted events
type ReplicationLog struct {
	mu    sync.Mutex
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
