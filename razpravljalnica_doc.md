# Razpravljalnica — Koraki in Koda (Podrobno)

## Vsebina repozitorija (predlog strukture)
```
razpravljalnica/
├─ cmd/
│  ├─ server/
│  │  └─ main.go
│  └─ client/
│     └─ main.go
├─ proto/
│  └─ razpravljalnica.proto
├─ pkg/
│  ├─ api/               # generated pb.go files live here (go package)
│  ├─ server/            # server logic, in-memory storage, replication
│  ├─ control/           # control plane (monitoring, node management)
│  ├─ client/            # client helper library (wrappers)
│  └─ common/            # common helpers, types, utils
├─ test/
│  └─ server_test.go
├─ go.mod
└─ README.md
```

---

## 1. Proto datoteka in generiranje Go kode
Ustvari `proto/razpravljalnica.proto` točno tako, kot si podal. Nato generiraj Go stub-e:
```bash
protoc --go_out=pkg/api --go-grpc_out=pkg/api proto/razpravljalnica.proto
```

Pomembno: nastavi `option go_package` v `.proto`:
```proto
option go_package = "github.com/tvoj_uporabnik/razpravljalnica/pkg/api;api";
```

---

## 2. In-memory modeli in ID generatorji
Datoteka: **pkg/server/storage.go**

```go
package server

import (
    "sync"
    "time"

    "github.com/tvoj_uporabnik/razpravljalnica/pkg/api"
    "google.golang.org/protobuf/types/known/timestamppb"
)

type User struct {
    ID   int64
    Name string
}

type Topic struct {
    ID   int64
    Name string
}

type Message struct {
    ID        int64
    TopicID   int64
    UserID    int64
    Text      string
    CreatedAt *timestamppb.Timestamp
    Likes     int32
}

// Global in-memory storage (per-node)
type NodeStorage struct {
    mu sync.RWMutex

    nextUserID    int64
    nextTopicID   int64
    nextMessageID int64

    users    map[int64]*User
    topics   map[int64]*Topic
    messages map[int64]map[int64]*Message

    subs map[int64]map[string]chan *api.MessageEvent
}

func NewNodeStorage() *NodeStorage {
    return &NodeStorage{
        nextUserID:    1,
        nextTopicID:   1,
        nextMessageID: 1,
        users:    make(map[int64]*User),
        topics:   make(map[int64]*Topic),
        messages: make(map[int64]map[int64]*Message),
        subs:     make(map[int64]map[string]chan *api.MessageEvent),
    }
}

func (s *NodeStorage) genUserID() int64 {
    s.nextUserID++
    return s.nextUserID - 1
}
func (s *NodeStorage) genTopicID() int64 {
    s.nextTopicID++
    return s.nextTopicID - 1
}
func (s *NodeStorage) genMessageID() int64 {
    s.nextMessageID++
    return s.nextMessageID - 1
}

func toPBMessage(m *Message) *api.Message {
    return &api.Message{
        Id:        m.ID,
        TopicId:   m.TopicID,
        UserId:    m.UserID,
        Text:      m.Text,
        CreatedAt: m.CreatedAt,
        Likes:     m.Likes,
    }
}
```

**Nasveti:** uporabi RWMutex; ID-ji so lokalni; messages je map-of-maps.

---

## 3. Implementacija osnovnega gRPC strežnika (MessageBoard)
Datoteka: **pkg/server/board.go**

_(Skrajšana, a delujoča koda)_

```go
package server

import (
    "context"
    "errors"
    "fmt"
    "io"
    "sync"
    "time"

    api "github.com/tvoj_uporabnik/razpravljalnica/pkg/api"
    "google.golang.org/protobuf/types/known/emptypb"
    "google.golang.org/protobuf/types/known/timestamppb"
)

type MessageBoardServer struct {
    api.UnimplementedMessageBoardServer

    storage *NodeStorage

    nodeID   string
    isHead   bool
    isTail   bool
    nextNode string

    seqMu   sync.Mutex
    nextSeq int64
}

func NewMessageBoardServer(nodeID string) *MessageBoardServer {
    return &MessageBoardServer{
        storage: NewNodeStorage(),
        nodeID:  nodeID,
        nextSeq: 1,
    }
}

func (s *MessageBoardServer) nextSequence() int64 {
    s.seqMu.Lock(); defer s.seqMu.Unlock()
    v := s.nextSeq; s.nextSeq++
    return v
}

func (s *MessageBoardServer) CreateUser(ctx context.Context, req *api.CreateUserRequest) (*api.User, error) {
    s.storage.mu.Lock(); defer s.storage.mu.Unlock()
    id := s.storage.genUserID()
    s.storage.users[id] = &User{ID: id, Name: req.Name}
    return &api.User{Id: id, Name: req.Name}, nil
}

func (s *MessageBoardServer) CreateTopic(ctx context.Context, req *api.CreateTopicRequest) (*api.Topic, error) {
    s.storage.mu.Lock(); defer s.storage.mu.Unlock()
    id := s.storage.genTopicID()
    s.storage.topics[id] = &Topic{ID: id, Name: req.Name}
    return &api.Topic{Id: id, Name: req.Name}, nil
}

func (s *MessageBoardServer) PostMessage(ctx context.Context, req *api.PostMessageRequest) (*api.Message, error) {
    s.storage.mu.Lock(); defer s.storage.mu.Unlock()

    if _, ok := s.storage.users[req.UserId]; !ok { return nil, errors.New("user not found") }
    if _, ok := s.storage.topics[req.TopicId]; !ok { return nil, errors.New("topic not found") }

    id := s.storage.genMessageID()
    now := timestamppb.Now()
    msg := &Message{ID: id, TopicID: req.TopicId, UserID: req.UserId, Text: req.Text, CreatedAt: now}

    if _, ok := s.storage.messages[req.TopicId]; !ok {
        s.storage.messages[req.TopicId] = make(map[int64]*Message)
    }
    s.storage.messages[req.TopicId][id] = msg

    ev := &api.MessageEvent{
        SequenceNumber: s.nextSequence(),
        Op:             api.OpType_OP_POST,
        Message:        toPBMessage(msg),
        EventAt:        timestamppb.Now(),
    }

    go s.broadcastToLocalSubscribers(req.TopicId, ev)

    // TODO: replicate to nextNode if head
    return toPBMessage(msg), nil
}

func (s *MessageBoardServer) broadcastToLocalSubscribers(topicID int64, ev *api.MessageEvent) {
    s.storage.mu.RLock(); defer s.storage.mu.RUnlock()
    subs, ok := s.storage.subs[topicID]; if !ok { return }
    for _, ch := range subs {
        select { case ch <- ev: default: }
    }
}

func (s *MessageBoardServer) SubscribeTopic(req *api.SubscribeTopicRequest, stream api.MessageBoard_SubscribeTopicServer) error {
    subID := fmt.Sprintf("%d-%d-%s", req.UserId, time.Now().UnixNano(), req.SubscribeToken)
    ch := make(chan *api.MessageEvent, 100)

    s.storage.mu.Lock()
    for _, tid := range req.TopicId {
        if _, ok := s.storage.subs[tid]; !ok {
            s.storage.subs[tid] = make(map[string]chan *api.MessageEvent)
        }
        s.storage.subs[tid][subID] = ch
    }
    s.storage.mu.Unlock()

    defer func() {
        s.storage.mu.Lock(); defer s.storage.mu.Unlock()
        for _, tid := range req.TopicId { delete(s.storage.subs[tid], subID) }
    }()

    for {
        select {
        case <-stream.Context().Done(): return stream.Context().Err()
        case ev := <-ch:
            if err := stream.Send(ev); err != nil { return err }
        }
    }
}

func (s *MessageBoardServer) ListTopics(ctx context.Context, _ *emptypb.Empty) (*api.ListTopicsResponse, error) {
    s.storage.mu.RLock(); defer s.storage.mu.RUnlock()
    resp := &api.ListTopicsResponse{}
    for _, t := range s.storage.topics {
        resp.Topics = append(resp.Topics, &api.Topic{Id: t.ID, Name: t.Name})
    }
    return resp, nil
}

func (s *MessageBoardServer) GetMessages(ctx context.Context, req *api.GetMessagesRequest) (*api.GetMessagesResponse, error) {
    s.storage.mu.RLock(); defer s.storage.mu.RUnlock()
    out := &api.GetMessagesResponse{}
    msgsMap, ok := s.storage.messages[req.TopicId]
    if !ok { return out, nil }
    for _, m := range msgsMap {
        if m.ID >= req.FromMessageId { out.Messages = append(out.Messages, toPBMessage(m)) }
    }
    return out, nil
}
```

---

## 4. Osnovna veriga (Chain Replication)

### Koncept
- Vsi zapisi gredo na **head**.
- Head replicira na nextNode → ... → tail.
- **Tail** potrjuje clientu.
- Branja gredo iz tail.

### Minimalna implementacija
V `.proto` dodaj:
```proto
message ReplicateRequest { MessageEvent event = 1; }
message ReplicateResponse { bool ok = 1; }
service InternalReplication {
  rpc Replicate(ReplicateRequest) returns (ReplicateResponse);
}
```

Psevdo-koda:
```go
if s.isHead && s.nextNode != "" {
    // RPC Replicate(event)
}
```

---

## 5. Nadzorna ravnina (Control Plane)

Naloge:
- vodi seznam node-ov
- heartbeat
- reconfig (odstrani nedelujoč node, preveže head → mid → tail)
- zahteva log replay od head

Statična verzija zadostuje za oceno **7–8**, polna dinamika + Raft za **9–10**.

---

## 6. Obvladovanje nezanesljivih vozlišč

Za najvišjo oceno dodaj:
- write-ahead log na head
- snapshot + state transfer
- failure detector

---

## 7. Avtorizacija in varnost
- TLS med node-i
- preverjanje user_id pri edit/delete
- možnost JWT ali certifikatov

---

## 8. Odjemalec (CLI)
Datoteka: **cmd/client/main.go**

```go
package main

import (
    "context"
    "fmt"
    "log"
    "os"
    "time"

    api "github.com/tvoj_uporabnik/razpravljalnica/pkg/api"
    "google.golang.org/grpc"
)

func main() {
    if len(os.Args) < 2 {
        fmt.Println("usage: client <server:port>")
        return
    }
    addr := os.Args[1]
    conn, err := grpc.Dial(addr, grpc.WithInsecure())
    if err != nil { log.Fatalf("failed connect: %v", err) }
    defer conn.Close()

    c := api.NewMessageBoardClient(conn)

    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    u, _ := c.CreateUser(ctx, &api.CreateUserRequest{Name: "David"})
    fmt.Println("created user:", u.Id)

    t, _ := c.CreateTopic(ctx, &api.CreateTopicRequest{Name: "Tema1"})

    m, _ := c.PostMessage(ctx, &api.PostMessageRequest{TopicId: t.Id, UserId: u.Id, Text: "Pozdrav!"})
    fmt.Println("posted msg id:", m.Id)

    streamCtx, streamCancel := context.WithCancel(context.Background())
    defer streamCancel()

    stream, _ := c.SubscribeTopic(streamCtx, &api.SubscribeTopicRequest{
        TopicId: []int64{t.Id}, UserId: u.Id, FromMessageId: 0, SubscribeToken: "token¨",
    })

    go func() {
        for {
            ev, err := stream.Recv()
            if err != nil {
                log.Printf("stream ended: %v", err)
                return
            }
            fmt.Println("EVENT", ev.Op, ev.Message.Text)
        }
    }()

    time.Sleep(10 * time.Second)
}
```

---

## 9. Testi
Datoteka: **test/server_test.go**

```go
package test

import (
    "context"
    "testing"

    server "github.com/tvoj_uporabnik/razpravljalnica/pkg/server"
    api "github.com/tvoj_uporabnik/razpravljalnica/pkg/api"
)

func TestCreateUserAndTopicAndPost(t *testing.T) {
    s := server.NewMessageBoardServer("node-1")
    ctx := context.Background()

    u, err := s.CreateUser(ctx, &api.Create

