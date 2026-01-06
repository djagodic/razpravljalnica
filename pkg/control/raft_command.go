package control

import (
	"encoding/json"

	nadzorna_ravnina "github.com/djagodic/razpravljalnica2/pkg/api/nadzornaRavnina"
)

// RPCCommand is the command stored in Raft log.
type RPCCommand struct {
	// Op is the operation name:
	// - RegisterNode
	// - DeregisterNode
	// - SetNodes (full control-plane state replication)
	Op string `json:"op"`

	RegisterNodeRequest *nadzorna_ravnina.RegisterNodeRequest `json:"register,omitempty"`
	NodeID              string                                `json:"node_id,omitempty"`
	// HeartbeatAt is unix nano timestamp used by Heartbeat op.
	HeartbeatAt         int64                                 `json:"heartbeat_at,omitempty"`

	// Nodes is used by the SetNodes operation.
	Nodes []*NodeInfo `json:"nodes,omitempty"`
}

// Marshal encodes command to bytes.
func (c *RPCCommand) Marshal() ([]byte, error) {
	return json.Marshal(c)
}

// Unmarshal decodes bytes into command.
func (c *RPCCommand) Unmarshal(data []byte) error {
	return json.Unmarshal(data, c)
}
