package common

import (
	"fmt"
	"sync/atomic"
)

var idCounter int64 = 1

// GenID generates unique IDs for users, topics, comments, etc.
func GenID(prefix string) string {
	id := atomic.AddInt64(&idCounter, 1)
	return fmt.Sprintf("%s-%d", prefix, id)
}
