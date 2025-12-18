package server

import (
	"context"
	"log"
	"time"
)

// ReplicateEntry replicates a log entry to next node in chain
func (s *MessageBoardServer) ReplicateEntry(entry *LogEntry) error {
	if s.nextNode == nil {
		// tail reached
		return nil
	}

	// prepare RPC call to next node
	client := s.nextNodeClient()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := client.Replicate(ctx, entry)
	if err != nil {
		log.Printf("Replication failed to node %s: %v", s.nextNode.ID, err)
		// notify control plane for reconfig
		s.notifyControlPlaneNextDown()
		return err
	}
	if !resp.Ok {
		log.Printf("Replication response not ok from node %s", s.nextNode.ID)
	}
	return nil
}

// notifyControlPlaneNextDown informs control plane about failed next node
func (s *MessageBoardServer) notifyControlPlaneNextDown() {
	if s.controlPlaneClient != nil {
		go s.controlPlaneClient.HandleNodeFailure(s.nextNode.ID)
	}
}

// ApplyLog applies log entry locally
func (s *MessageBoardServer) ApplyLog(entry *LogEntry) {
	switch entry.Op {
	case OpCreateUser:
		s.storage.AddUser(entry.User)
	case OpCreateTopic:
		s.storage.AddTopic(entry.Topic)
	case OpCreateComment:
		_ = s.storage.AddComment(entry.Comment)
	case OpUpdateComment:
		_ = s.storage.UpdateComment(entry.Comment)
	case OpDeleteComment:
		_ = s.storage.DeleteComment(entry.Comment.TopicID, entry.Comment.ID, entry.Comment.UserID)
	case OpLikeComment:
		_, _ = s.storage.LikeComment(entry.Comment.TopicID, entry.Comment.ID)
	}
}
