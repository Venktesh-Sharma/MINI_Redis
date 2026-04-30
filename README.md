# Mini-Redis

A production-quality Redis-inspired in-memory key-value store built in Java 17.  
Designed as a strong resume/portfolio project for backend and distributed systems roles.

---

## Architecture Overview

```
┌────────────────────────────────────────────────────────────┐
│                      TCP Server (port 6399)                │
│              (CachedThreadPool — one thread/client)        │
└─────────────────────────┬──────────────────────────────────┘
                          │ Socket I/O per client
                          ▼
┌────────────────────────────────────────────────────────────┐
│               CommandProcessor (per-connection)            │
│         Parses raw text → dispatches to subsystems         │
└────────┬─────────────────┬──────────────────┬─────────────┘
         │                 │                  │
         ▼                 ▼                  ▼
┌─────────────┐   ┌─────────────────┐  ┌────────────────┐
│  DataStore  │   │  AofPersistence │  │  PubSubBroker  │
│  (shared)   │   │  (append-only)  │  │  (shared)      │
│             │   │                 │  │                │
│  LRUCache   │   │  Replay on boot │  │  ConcurrentMap │
│  TTL Map    │   │  Compaction     │  │  channel→list  │
│  RW Lock    │   │  every 5 min    │  │  of listeners  │
└─────────────┘   └─────────────────┘  └────────────────┘
```

### Key Design Decisions

| Concern | Solution |
|---|---|
| Thread safety | `ReentrantReadWriteLock` in DataStore; `ConcurrentHashMap` for TTL + PubSub |
| LRU eviction | `LinkedHashMap` in access-order mode with `removeEldestEntry` override |
| Persistence | Append-Only File (AOF) with periodic compaction (atomic rename) |
| TTL cleanup | Lazy expiry on read + background `ScheduledExecutorService` sweep every 500ms |
| PubSub | `CopyOnWriteArrayList` per channel — safe concurrent publish + subscribe |

---

## Folder Structure

```
mini-redis/
├── pom.xml
└── src/
    ├── main/java/com/miniredis/
    │   ├── MiniRedisServer.java          ← Bootstrap / main()
    │   ├── core/
    │   │   ├── DataStore.java            ← SET/GET/DEL/EXPIRE/INCR/TTL
    │   │   └── ValueEntry.java           ← Value wrapper
    │   ├── eviction/
    │   │   └── LRUCache.java             ← LinkedHashMap-based LRU
    │   ├── persistence/
    │   │   └── AofPersistence.java       ← AOF log + replay + compaction
    │   ├── pubsub/
    │   │   └── PubSubBroker.java         ← Pub/Sub channel broker
    │   ├── commands/
    │   │   └── CommandProcessor.java     ← Command parser + dispatcher
    │   ├── network/
    │   │   └── TcpServer.java            ← TCP accept loop + ClientHandler
    │   └── cli/
    │       └── MiniRedisClient.java      ← Interactive CLI client
    └── test/java/com/miniredis/
        ├── DataStoreTest.java            ← Unit tests (CRUD, TTL, concurrency, LRU)
        └── CommandProcessorTest.java     ← Integration tests for all commands
```

---

## Data Structures

| Structure | Where Used | Why |
|---|---|---|
| `LinkedHashMap` (access-order) | `LRUCache` | O(1) get/put + automatic LRU eviction |
| `ConcurrentHashMap` | TTL map, PubSub channels | Lock-free concurrent reads |
| `CopyOnWriteArrayList` | PubSub listener lists | Safe iteration during concurrent publish |
| `ReentrantReadWriteLock` | DataStore | Multiple concurrent readers, exclusive writers |
| `ScheduledExecutorService` | TTL cleaner, AOF compactor | Lightweight background threads |
| `CachedThreadPool` | TCP server | Dynamic thread allocation per client |

---

## Build & Run

### Prerequisites
- Java 17+
- Maven 3.8+

### Build
```bash
cd mini-redis
mvn clean package -q
```

### Run Server
```bash
# Default port 6399, max 10,000 keys
java -jar target/miniredis.jar

# Custom port and capacity
java -jar target/miniredis.jar 6399 50000
```

### Run CLI Client
```bash
# In another terminal
java -cp target/miniredis.jar com.miniredis.cli.MiniRedisClient

# Connect to custom host/port
java -cp target/miniredis.jar com.miniredis.cli.MiniRedisClient 127.0.0.1 6399
```

### Or connect with netcat (no client needed)
```bash
nc localhost 6399
```

### Run Tests
```bash
mvn test
```

---

## Example Session

```
mini-redis> PING
PONG

mini-redis> SET name Venktesh
OK

mini-redis> GET name
"Venktesh"

mini-redis> SET visits 0
OK

mini-redis> INCR visits
(integer) 1

mini-redis> INCR visits
(integer) 2

mini-redis> INCRBY visits 10
(integer) 12

mini-redis> SET session_token abc123 EX 30
OK

mini-redis> TTL session_token
(integer) 29

mini-redis> EXISTS session_token
(integer) 1

mini-redis> DEL session_token
(integer) 1

mini-redis> EXISTS session_token
(integer) 0

mini-redis> KEYS
*3
  name
  visits
  ...

mini-redis> DBSIZE
DBSIZE 2

mini-redis> SUBSCRIBE news
SUBSCRIBED news (1 subscribers)

# In another terminal:
mini-redis> PUBLISH news "Breaking: Mini-Redis launched!"
PUBLISHED to 1 subscriber(s)

# Back in subscriber terminal, message appears:
*MESSAGE news: Breaking: Mini-Redis launched!

mini-redis> FLUSHALL
OK

mini-redis> QUIT
BYE
```

---

## Possible Improvements

1. **RESP Protocol** — Implement Redis's binary-safe RESP3 protocol for compatibility with existing Redis clients (`redis-cli`, Jedis, Lettuce)
2. **Data Types** — Add List (LPUSH/RPOP), Hash (HSET/HGET), Set (SADD/SMEMBERS), Sorted Set (ZADD/ZRANGE)
3. **Persistence: RDB Snapshots** — Point-in-time binary snapshots for faster recovery (complement AOF)
4. **Cluster/Replication** — Leader-follower replication with RAFT consensus; consistent hashing for sharding
5. **Pipelining** — Batch multiple commands in one TCP roundtrip
6. **AUTH / ACL** — Password authentication and role-based command access control
7. **Transactions** — MULTI/EXEC with optimistic locking (WATCH)
8. **Pattern Matching** — `KEYS user:*` with glob-style pattern matching
9. **Metrics** — Prometheus-compatible `/metrics` HTTP endpoint (commands/sec, hit rate, memory)
10. **Lua Scripting** — EVAL command for atomic server-side scripts

---

## Why This Is a Strong Resume Project

- **Distributed systems fundamentals**: concurrency, locking, thread-per-connection model
- **Data structure mastery**: LRU via LinkedHashMap, TTL via concurrent maps
- **Persistence engineering**: AOF write-ahead log with atomic compaction
- **Event-driven design**: Pub/Sub with async listener dispatch
- **Clean OOP**: Single-responsibility classes, dependency injection, no god objects
- **Observable**: Structured logging throughout; ready for Prometheus integration
- **Testable**: Pure unit tests + integration tests, no network mocking needed for core logic
