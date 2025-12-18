package server

import (
	"time"

	api "github.com/djagodic/razpravljalnica/pkg/api/razpravljalnica"
)

// User model
type User struct {
	ID   string
	Name string
}

// Topic model
type Topic struct {
	ID   string
	Name string
}

// Comment model
type Comment struct {
	ID        string
	TopicID   string
	UserID    string
	Body      string
	Timestamp time.Time
	Likes     int32
}

// Convert internal Comment to proto Comment
func ToProtoComment(c *Comment) *api.Comment {
	return &api.Comment{
		Id:        c.ID,
		TopicId:   c.TopicID,
		UserId:    c.UserID,
		Body:      c.Body,
		Timestamp: c.Timestamp.Unix(),
	}
}
