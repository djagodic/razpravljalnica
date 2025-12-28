package server

//TODO ali se tole sploh uporablja
type Node struct {
	ID      string
	Address string
	IsHead  bool
	IsTail  bool
	Next    *Node
	Prev    *Node
}
