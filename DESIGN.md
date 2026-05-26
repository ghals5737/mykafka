# MyKafka — MVP 설계 & 구현 문서

> 목적: Kafka의 **아이덴티티와 코어 로직**을 직접 구현하며 학습.
> 상태: **MVP 완성** (Step 1 ~ 8 모두 동작 검증). 분산/복제는 범위 밖.

---

## 1. Kafka의 아이덴티티

Kafka는 메시지 큐가 아니라 **"분산 커밋 로그(distributed commit log)"** 다.
이 한 줄이 모든 설계 결정의 출발점이다.

| 통념 (전통 MQ: RabbitMQ 등)     | Kafka의 선택                  | 왜                                                        |
| ------------------------------- | ----------------------------- | --------------------------------------------------------- |
| 메시지를 큐에서 빼면 사라짐     | 로그에 영구 append, 안 지움    | 여러 컨슈머가 같은 데이터를 **재처리/재구독** 가능        |
| 메모리 기반이 빠름              | **디스크 기반**               | Sequential disk I/O + OS 페이지 캐시 활용. 충분히 빠름    |
| 브로커가 컨슈머에게 push        | 컨슈머가 pull                 | 컨슈머가 **자기 속도대로** 가져감 → 백프레셔 자동 해결    |
| 브로커가 컨슈머 상태 관리       | **컨슈머가 offset 들고 다님** | 브로커는 stateless에 가까움 → 수평 확장 용이              |
| 메시지에 ID 부여 (UUID 등)      | **offset = 위치이자 ID**      | 단조 증가 정수, 인덱싱 O(1), 별도 ID 생성기 불필요        |
| 컨슈머 상태를 별도 저장소에 보관| 내부 토픽 `__consumer_offsets`| "모든 게 로그" — broker가 자기 메커니즘을 dogfooding      |

---

## 2. 기술 스택

- 언어: **Kotlin 2.2.x**
- JVM: **17** (로컬 JDK 가용성에 맞춤)
- 네트워크: **Netty 4.1.x**
- 빌드: **Gradle (Kotlin DSL)**
- 저장: 로컬 파일시스템 (`FileChannel`, mmap)

실행:
```bash
./gradlew run --args="9092 data <maxSegmentBytes> <indexIntervalBytes>"
# 예) ./gradlew run --args="9092 data 200 50"   # 작은 segment + 작은 index interval
```

---

## 3. 디렉토리 구조 (현재)

```
MyKafka/
├── build.gradle.kts
├── settings.gradle.kts
├── DESIGN.md
├── data/                                    # 런타임 생성, .gitignore
│   ├── <topic>/
│   │   └── <partitionId>/
│   │       ├── 00000000000000000000.log
│   │       ├── 00000000000000000000.index
│   │       └── ...
│   └── __consumer_offsets/                  # 내부 토픽 (Step 8)
│       └── 0/
│           └── 00000000000000000000.log
└── src/main/kotlin/com/example/mykafka/
    ├── Main.kt
    ├── server/
    │   ├── BrokerServer.kt                  # Netty boss/worker
    │   └── RequestRouter.kt                 # ApiKey 분기
    ├── protocol/
    │   ├── ApiKey.kt                        # PRODUCE/FETCH/CREATE_TOPIC/COMMIT_OFFSET/FETCH_OFFSET
    │   ├── Frame.kt
    │   └── FrameCodec.kt                    # length-prefix framing
    ├── log/
    │   ├── Record.kt
    │   ├── RecordCodec.kt                   # [offset|ts|keyLen|key|valLen|value|crc]
    │   ├── Segment.kt                       # 단일 .log + .index
    │   ├── OffsetIndex.kt                   # sparse mmap index
    │   └── Log.kt                           # 다수 segment 관리 (1 파티션)
    └── topic/
        ├── LogManager.kt                    # topic ↔ List<Log> + partitioner
        └── OffsetStore.kt                   # 컨슈머 offset 영속화
```

---

## 4. Wire Protocol

모든 메시지는 length-prefix framed.

```
[4 bytes totalLength][1 byte apiKey][payload]
totalLength = 1(apiKey) + payload 길이
```

### apiKey 표

| apiKey | 이름            | 도입 |
|-------:|-----------------|------|
| 0      | PRODUCE         | Step 1 (echo) → 2 (append) → 6 (batch) |
| 1      | FETCH           | Step 4 (단건) → 7 (batch pull) |
| 2      | CREATE_TOPIC    | Step 5 |
| 3      | COMMIT_OFFSET   | Step 8 |
| 4      | FETCH_OFFSET    | Step 8 |

### CREATE_TOPIC
- req: `[topicLen:2][topic][partitionCount:4]`
- resp: `[status:1]`  (0=OK, 1=ALREADY_EXISTS, 2=INVALID)
- 토픽명 규칙: `[A-Za-z0-9_-]{1,64}`

### PRODUCE (batch)
- req: `[topicLen:2][topic][partition:4][recordCount:4]` + 각 record `[keyLen:4][key][valueLen:4][value]`
  - `partition == -1` → broker가 partitioner로 결정 (첫 record의 key 기준)
  - `keyLen == -1` → null key
- resp: `[errCode:1][partition:4][baseOffset:8][count:4]`
  - errCode: 0=OK, 1=UNKNOWN_TOPIC, 2=UNKNOWN_PARTITION, 3=INVALID_BATCH
  - 개별 record offset = `baseOffset, baseOffset+1, …, baseOffset+count-1`

### FETCH (pull-based batch)
- req: `[topicLen:2][topic][partition:4][offset:8][maxBytes:4]`
- resp: `[errCode:1][recordCount:4][record1 bytes][record2 bytes]…`
  - record는 RecordCodec 포맷 그대로 (self-delimiting)
  - errCode: 0=OK, 1=UNKNOWN_TOPIC_OR_PARTITION
  - **진행 보장**: record 1건 크기가 maxBytes보다 커도 1건은 무조건 포함

### COMMIT_OFFSET
- req: `[groupLen:2][group][topicLen:2][topic][partition:4][offset:8]`
- resp: `[errCode:1]`

### FETCH_OFFSET
- req: `[groupLen:2][group][topicLen:2][topic][partition:4]`
- resp: `[errCode:1][offset:8]`
  - offset = -1 → 그 group이 그 (topic, partition)을 commit한 적 없음

---

## 5. On-Disk Record 포맷

```
[offset:8][timestamp:8][keyLen:4][key:keyLen][valueLen:4][value:valueLen][crc:4]
```

- `keyLen == -1` → null key
- `crc` = `offset` 시작부터 `value` 끝까지 바이트에 대한 CRC32

### .index 파일

```
[relativeOffset:4][filePosition:4]   ← 8 byte entries, sparse
```

- relativeOffset = `recordOffset - segment.baseOffset` (segment 안에서만 유효)
- mmap 으로 잡힘
- 시작 시 .log를 스캔해 항상 재구축 (인덱스는 파생 정보)

---

## 6. 8단계 — 무엇을 만들었고 왜 그렇게 만들었나

### Step 1 — Netty TCP 서버 + length-prefix framing

**무엇:** `FrameDecoder`/`FrameEncoder`, boss/worker `EventLoopGroup`, apiKey 분기 스켈레톤.

**왜:**
- HTTP 안 씀 — Kafka는 고스루풋 영속 연결 전제. raw TCP + 자체 binary protocol이 적합.
- length-prefix — TCP는 바이트 스트림. 메시지 경계를 길이로 표현해야 receiver가 잘라 읽음.
- Netty `ByteToMessageDecoder` — partial-read/fragmentation을 알아서 처리.

---

### Step 2 — Append-only Log 파일 (Kafka의 심장)

**무엇:** `FileChannel` 기반 append-only log. broker가 offset 부여. CRC 검증. 시작 시 처음부터 스캔해 `nextOffset` 복구, 깨진 trailing record는 truncate.

**왜:**
- **Sequential write는 random write보다 디스크에서 ~100배 빠름.** HDD에선 헤드 이동 없음, SSD에선 write amplification 적음.
- **Immutable이라 락이 거의 필요 없음.** 쓰기는 append 한 곳에만, 읽기는 어디서나.
- **OS 페이지 캐시 위임.** JVM heap에 별도 캐시 안 만들어도 됨.
- **broker-assigned offset.** 클라이언트가 정하면 동시 producer가 충돌. broker가 single source of truth.
- **꼬리 자르기 복구.** 부분 쓰기된 trailing record는 신뢰하지 않고 잘라낸다 (실 Kafka도 동일).

---

### Step 3 — Log Segment 분할

**무엇:** 단일 파일이 무한정 커지지 않도록 일정 크기 넘으면 새 segment 파일로 롤링. 파일명 = `%020d.log`(첫 offset).

**왜:**
- **Retention 삭제가 O(1).** record를 하나씩 지우는 게 아니라 segment 파일 통째로 unlink. 단편화 없음.
- **파일명 = baseOffset → 디렉토리 자체가 인덱스.** 임의 offset이 어느 파일에 있는지 파일명만으로 이진 탐색.
- **활성 segment만 쓰기, 나머지는 read-only.** 파일시스템/mmap 최적화에 유리.

---

### Step 4 — Sparse offset index (mmap)

**무엇:** segment마다 `.index` 파일 (`[relOffset:4][filePos:4]` sparse 엔트리). mmap으로 매핑. lookup은 이진 탐색 + 짧은 sequential decode.

**왜:**
- **dense 인덱스는 record 수에 비례.** 메모리 부담 + write 시 인덱스도 매번 갱신.
- **sparse + 짧은 sequential scan.** 순차 disk read는 OS readahead로 매우 빠름.
- **mmap.** 인덱스는 작고 자주 읽힘 → OS 페이지 캐시 위임.
- **자기 완결성.** `.index`가 망가져도 `.log`만 있으면 완전 재구축 가능. 검증에서 index 전부 0으로 깨도 정상 동작.

---

### Step 5 — Topic & Partition

**무엇:** `LogManager`가 토픽 → `List<Log>` 매핑 관리. 디렉토리 레이아웃 `data/<topic>/<partitionId>/`. partitioner: key 있음 → `hash(key) % N`, key 없음 → atomic round-robin.

**왜:**
- **파티션 = 병렬성 + 순서 보장의 단위.**
  - 같은 key는 같은 partition → 그 entity의 이벤트 **순서 보장**.
  - 다른 key는 분산 → **병렬 처리** 가능.
  - 의도적 trade-off: 토픽 전역 순서 ✗, 파티션 내 순서 ✓.
- **파티션마다 별도 Log 인스턴스.** 락은 각 Log의 append에만 → **파티션 간 락 충돌 0**.
- **디렉토리 = 단일 진실 출처.** 메타파일 없이 디렉토리 트리만으로 모든 정보 복구.

---

### Step 6 — PRODUCE batch

**무엇:** 한 요청에 N개의 record. 한 번의 `channel.write`로 묶고, baseOffset+count 응답.

**왜:**
- **네트워크 RTT, 락 획득/해제, syscall, 인덱스 갱신 비용**은 record 수가 아니라 batch 수에 비례.
- **all-or-none atomic append** — 단일 락 + 단일 write. 부분 성공 없음.
- **연속 offset 보장** — base + 0..N-1. 클라이언트는 두 정수만으로 모든 record offset을 안다.

**측정**: 1000건 1×1 vs 1×1000
- 1000회 단건 PRODUCE: **84ms**
- 1회 1000-record batch: **1.4ms**
- → **61배 throughput 차이** (Kafka가 "수십만 msgs/sec" 내는 비밀)

---

### Step 7 — FETCH batch (pull-based)

**무엇:** `read(offset, maxBytes)` → 시작 offset부터 maxBytes 한도 안에서 가능한 record를 모음. segment 경계 자동으로 넘어감.

**왜:**
- **Pull-based.** broker는 "이 offset부터 maxBytes" 요청에만 응답. 컨슈머가 자기 페이스 결정.
- **Consumer-managed offset.** broker는 컨슈머별 상태 추적 X. 같은 offset으로 두 번 fetch하면 같은 결과 → 재처리 자유.
- **거의 stateless broker.** 컨슈머 수에 무관하게 broker 메모리/상태 거의 일정.
- **진행 보장.** record가 maxBytes보다 커도 1건은 보냄. 안 그러면 컨슈머 영원히 막힘 (Kafka 동일 invariant).

**측정**: 50건을 maxBytes=80으로 pull → 컨슈머가 25 라운드에 걸쳐 모두 수신, 매 라운드마다 자기 offset 갱신.

---

### Step 8 — Consumer offset 영속화 (내부 토픽 방식)

**무엇:** 시작 시 `__consumer_offsets` 토픽(1 파티션) 자동 생성. COMMIT은 그 토픽에 record append (`key = encode(group, topic, partition)`, `value = offset:8`). 시작 시 처음부터 스캔해 in-memory cache 재구축.

**왜:**
- **"모든 게 로그"** — 별도 메타 저장소 없이, broker가 이미 가진 메커니즘 그대로 재활용.
- **단일 메커니즘 재활용.** 새 코드는 key/value 인코딩 + cache뿐. Log.append/readAll/recovery 그대로 사용.
- **durability 동일.** 사용자 메시지와 같은 append-only + recovery 모델.
- **log compaction의 자연스러운 의미.** 같은 key가 반복 commit되면 옛 record는 garbage — compaction 도입 시 최신값만 살아남음.
- **컨슈머 죽었다 살아나도 group 진도 살아남음.** broker stateless를 유지하면서도 durable한 consumer state.

**측정**: 5번 commit한 group이 unique key는 2개 → 옛 record 3개는 "역사적 정보". 재시작 후 `loaded 5 commits (2 unique keys)` 로그가 그대로 보임.

---

## 7. 완성된 Kafka 정체성 5가지

| # | 정체성 | 구현 위치 |
|---|---|---|
| 1 | Append-only immutable log | Step 2 (`Log.append`) |
| 2 | Disk-first + sequential I/O | Step 2 (`FileChannel`, append at end) |
| 3 | Pull-based consumer | Step 7 (`Log.read`, FETCH protocol) |
| 4 | Consumer-managed offset (durable) | Step 7 client-side + Step 8 internal topic |
| 5 | Partition as unit of parallelism & ordering | Step 5 (`LogManager`, partitioner) |

---

## 8. 검증 매트릭스 (요약)

| 시나리오 | 결과 |
|---|---|
| PRODUCE 라운드트립, monotonic offset | ✅ |
| 재시작 후 nextOffset 정확 복구 | ✅ |
| 부분 쓰기 → 꼬리 자르기 복구 | ✅ |
| Segment 롤링 (파일명 = baseOffset) | ✅ |
| Multi-segment recovery | ✅ |
| Sparse mmap index, 임의 offset 빠르게 find | ✅ |
| 모든 .index 0으로 깨도 .log에서 재구축 | ✅ |
| 같은 key sticky partition | ✅ |
| 30 keys → 3 파티션 10/10/10 분산 | ✅ |
| null key 엄격 round-robin | ✅ |
| 토픽/파티션 디렉토리 자동 복구 | ✅ |
| Batch PRODUCE all-or-none | ✅ |
| 1000건 single vs 1×1000 batch → **61x speedup** | ✅ |
| Cross-segment FETCH | ✅ |
| `maxBytes=1` 진행 보장 (1건은 무조건) | ✅ |
| Consumer loop (25 round, 50건 수신, self-offset) | ✅ |
| `__consumer_offsets` 자동 생성 | ✅ |
| 5 commits → unique key 2개, 최신 우선 | ✅ |
| 재시작 → 컨슈머 group이 정확히 마지막 commit에서 이어 시작 | ✅ |

---

## 9. 의도적으로 안 넣은 것 (그리고 다음에 갈 수 있는 길)

| 영역 | MVP에서 생략 | 다음 단계 학습 포인트 |
|---|---|---|
| Replication | 단일 노드 | Leader/Follower + ISR + acks=all |
| Controller | 메타 관리 단순 | KRaft (Raft 기반 컨트롤러) |
| Log compaction | sparse만 구현 | 같은 key 옛 record 삭제 — `__consumer_offsets` 정리에 즉시 활용 가능 |
| Exactly-once | at-least-once | producer idempotence + transactions |
| Zero-copy | byte copy로 구현 | `sendfile()` / Netty `FileRegion` |
| Group coordination | "group" 문자열만 사용 | rebalance, member tracking, sticky assignment |
| Durability tuning | OS flush 의존 | `channel.force()` 정책 (per-request / per-N / time-based) |

각 항목 모두 MVP 위에 점진적으로 쌓아 올릴 수 있다.

---

## 10. 실행 / 디버깅 팁

```bash
# 일반 실행
./gradlew run --args="9092 data"

# segment 롤링/sparse index 효과를 작은 데이터로 보고 싶을 때
./gradlew run --args="9092 data 200 50"

# 디스크 상태 확인
find data -type f -name "*.log" -o -name "*.index" | sort
ls -la data/__consumer_offsets/0/

# 빌드만
./gradlew build
```

CLI 인자:
```
args[0] = port               (기본 9092)
args[1] = dataDir            (기본 "data")
args[2] = maxSegmentBytes    (기본 1MB)
args[3] = indexIntervalBytes (기본 4KB)
```
