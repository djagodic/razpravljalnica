# Razpravljalnica – porazdeljena razpravljalnica z nadzorno ravnino

Projekt **Razpravljalnica** implementira porazdeljeno razpravljalnico z:
- veriženo replikacijo podatkovnih strežnikov (head → tail),
- centralno nadzorno ravnino (control plane),
- podporo za odpornost na odpovedi komponent.

Projekt obstaja v **dveh različicah**, ki sta ločeni po Git branchih:
- **brez Rafta** (enostavna, centralizirana nadzorna ravnina),
- **z Raftom** (porazdeljena, fault-tolerant nadzorna ravnina).

---

## 🌿 Branchi v repozitoriju

| Branch | Opis |
|------|------|
| `main` | Stabilna verzija **brez Rafta** (centralizirana kontrolna ravnina) |
| `raft-control-plane` | Napredna verzija **z Raft nadzorno ravnino** (fault-tolerant) |

---

## 🧩 Arhitektura sistema

### Podatkovna ravnina
- Več **podatkovnih strežnikov** (MessageBoardServer)
- Povezani v **verigo replikacije**:
  - *head* sprejema zapise,
  - *tail* vrača potrjene rezultate.
- Podatki se replicirajo naprej po verigi.

### Nadzorna ravnina
- Skrbi za:
  - registracijo podatkovnih strežnikov,
  - zaznavanje odpovedi (heartbeat),
  - rekonfiguracijo verige (nov head/tail),
  - obveščanje strežnikov o spremembah.

---

## 🔹 Verzija brez Rafta (branch: `main`)

### Lastnosti
- Ena instanca kontrolne ravnine.
- Enostavna implementacija.
- Primerna za:
  - razvoj,
  - razumevanje osnov arhitekture,
  - okolja brez zahtev po visoki razpoložljivosti.

### Omejitve
- Kontrolna ravnina je **single point of failure**.
- Odpoved kontrolne ravnine pomeni nedelovanje sistema.

### Zagon (primer)
```bash
# kontrolna ravnina
go run ./cmd/control/main.go

# podatkovni strežnik
go run ./cmd/server/main.go -addr 127.0.0.1:50051 -id node-1

