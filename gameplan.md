**Korak 0: Priprava okolja**
Namesti Go (najbolje 1.21+).
Namesti gRPC in protobuf podpora za Go:

    go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest


Dodaj $GOPATH/bin v PATH.
Ustvari projekt:

    mkdir razpravljalnica
    cd razpravljalnica
    go mod init github.com/tvoj_uporabnik/razpravljalnica

Dodaj mapo za proto datoteke, npr. proto/.

###
**Korak 1: Definicija proto datotek**
V mapo proto/ dodaj razpravljalnica.proto s kodo, ki si jo podal.
Generiraj Go kodo:

    protoc --go_out=. --go-grpc_out=. proto/razpravljalnica.proto

To bo ustvarilo razpravljalnica.pb.go in razpravljalnica_grpc.pb.go.

###
**Korak 2: Modeli in in-memory baza**
V Go definira strukture za:

    User, Topic, Message

Shranjevanje v mapi / slice:

    var users = map[int64]*User{}
    var topics = map[int64]*Topic{}
    var messages = map[int64][]*Message{} // key = topic_id

Dodaj generator ID-jev (int64) za uporabnike, teme in sporočila.
Implementiraj mutex za varno hkratno delo:

    var mu sync.RWMutex

###
**Korak 3: Implementacija gRPC strežnika**
Ustvari server.go in implementiraj MessageBoardServer:
- CreateUser: ustvari user in vrni id.
- CreateTopic: ustvari topic.
- PostMessage: dodaj sporočilo v messages[topic_id].
- UpdateMessage in DeleteMessage: preveri user_id, uporabi mutex.
- LikeMessage: povečaj likes.
- ListTopics in GetMessages: uporabi RLock za branje.
- SubscribeTopic: implementiraj stream:
  - Shrani kanal (chan MessageEvent) za vsakega naročenega uporabnika.
  - Ko pride novo sporočilo, pošlji v vse kanale naročnikov.

Implementiraj ControlPlaneServer (za kasnejšo podporo replikaciji):
Vrni head in tail node (zaenkrat lahko samo 1 strežnik).

###
**Korak 4: gRPC odjemalec**
Ustvari client.go.
Poveži se s strežnikom:

    conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
    client := razpravljalnica.NewMessageBoardClient(conn)

Implementiraj funkcije:
    createUser(), createTopic(), postMessage(), subscribeTopic() (stream)


Dodaj CLI ali minimalni GUI z tview za testiranje.
###
**Korak 5: Enostavna veriga (chain replication)**
Razdeli strežnike v verigo:

    head → middle → tail.

Pisanje gre na head, ki replicira sporočila naprej.
Branje gre na tail.
Shrani vse podatke na vseh vozliščih (do enega nivoja dovolj za 7-8).
Naročnine se obravnavajo na head (pošlje novim uporabnikom/strežnikom).

###
**Korak 6: Obvladovanje odpovedi in dynamic chain**
Nadgradnja nadzorne ravnine (ControlPlane):
- Periodično preverjanje zdravja node-ov (heartbeat).
- V primeru odpovedi:
  - Odstrani node iz verige.
  - Ponovno poveži preostale node-e.
  - Head mora ponovno poslati ne-replicirana sporočila.

Dodaj možnost dodajanja novih node-ov v verigo.
Razmisli o uporabi Raft-a za nadzorno ravnino (npr. HashiCorp Raft).

###
**Korak 7: Testiranje in demonstracija**
Implementiraj enote testov v Go (*_test.go):
Kreiranje uporabnikov, tem, sporočil.
Posamezni CRUD testi.
Naročnine in stream.

Demo:
Zaženi 3 node verigo.
Ustvari nekaj uporabnikov in tem.
Pošlji sporočila in pokaži naročnine.
Simuliraj odpoved node-a in dodaj nov node.

Opcijsko: CLI ali minimalni GUI za interaktivno testiranje.

###
**Korak 8: Bonus**
CLI z cobra ali kong za odjemalca.
GUI z tview za strežnik in odjemalca.
Dokumentacija in README s primeri uporabe.

GitHub repo - Modularna struktura:

    /cmd/server
    /cmd/client
    /pkg/razpravljalnica
    /proto
    /test

CI/CD za avtomatsko testiranje (opcijsko).

###
**Priporočeni Go paketi**
gRPC: `google.golang.org/grpc`
protobuf: `google.golang.org/protobuf`
mutex / sync: `sync`
CLI: `cobra` ali `kong`
GUI: `github.com/rivo/tview`
Raft: `github.com/hashicorp/raft (za nadzorno ravnino)`