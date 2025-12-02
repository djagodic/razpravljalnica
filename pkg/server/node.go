package server

type Node struct {
	ID      string
	Address string
	IsHead  bool
	IsTail  bool
	Next    *Node
	Prev    *Node
}
