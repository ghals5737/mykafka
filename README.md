# MyKafka

Kafka의 **코어 아이덴티티와 핵심 로직을 직접 구현**하며 배우는 학습용 카프카 클론 (Kotlin).
"분산 커밋 로그(distributed commit log)"라는 Kafka의 정체성 — append-only 로그, pull 기반 컨슈머,
컨슈머가 들고 다니는 offset, 파티션 = 병렬성/순서의 단위 — 을 라이브러리로 패키징했습니다.

> ⚠️ **학습용 MVP**입니다. 단일 노드, 복제/KRaft/정확히-한-번은 범위 밖. 프로덕션 X.

## 모듈

| 모듈 | 내용 | 의존 |
|---|---|---|
| `protocol` | 와이어 포맷 (`ApiKey`, `Record`, `RecordCodec`) | kotlin-stdlib만 |
| `client` | **Producer / Consumer SDK** (raw Socket, netty 없음) | `protocol` |
| `broker` | Netty 기반 단일 노드 서버 | `protocol` + netty |

앱(프로듀서/컨슈머)은 보통 **`client`만** 의존하면 됩니다. broker는 별도 프로세스로 실행.

## 설치 (JitPack)

> 아래 `ghals5737`은 이 레포가 올라간 **GitHub 계정/조직으로 바꾸세요**. `TAG`는 릴리스 태그(예: `v0.1.0`).

```kotlin
// settings.gradle.kts 또는 build.gradle.kts
repositories {
    mavenCentral()
    maven("https://jitpack.io")
}

dependencies {
    implementation("com.github.ghals5737.mykafka:client:v0.1.0")
    // 브로커를 코드에서 띄우고 싶다면:
    // implementation("com.github.ghals5737.mykafka:broker:v0.1.0")
}
```

## 빠른 시작 — 토이 프로젝트에 SDK처럼 (브로커 임베드)

브로커를 따로 띄우기 귀찮으면 **broker 모듈도 의존성에 넣고 코드에서 데몬 스레드로 임베드**하면
한 프로세스로 끝납니다. (Kafka도 브로커는 필요하지만, 여기선 코드에 박을 수 있어 토이용으로 편함.)

```kotlin
// build.gradle.kts
//   implementation("com.github.ghals5737.mykafka:client:v0.1.0")
//   implementation("com.github.ghals5737.mykafka:broker:v0.1.0")

import com.example.mykafka.client.MyKafkaConsumer
import com.example.mykafka.client.MyKafkaProducer
import com.example.mykafka.server.BrokerServer
import java.nio.file.Path

fun main() {
    // 1) 브로커 임베드 (데몬 스레드로 띄우고 잠깐 대기)
    Thread { BrokerServer(port = 9092, dataDir = Path.of("data")).start() }
        .apply { isDaemon = true; start() }
    Thread.sleep(500)

    // 2) 발행
    MyKafkaProducer().use { p ->
        p.createTopic("events", partitionCount = 1)  // 토이: 단일 파티션 단순화
        p.produce("events", key = null, value = "hello", partition = 0)
    }

    // 3) 소비
    MyKafkaConsumer(group = "my-app").use { c ->
        val r = c.fetch("events", partition = 0, offset = 0)
        r.records.forEach { println(String(it.value)) }     // → hello
        c.commitOffset("events", partition = 0, offset = r.nextOffset)
    }
}
```

> 실제 서비스에 붙이는 예는 더 아래 [사용 예제](#사용-예제)(파티션·key 라우팅, consumer group)를 참고.
> broker를 임베드하지 않고 **별도 프로세스**로 띄우려면 아래 [브로커 실행](#브로커-실행)을 보세요.

## 사용 예제

### Producer
```kotlin
import com.example.mykafka.client.MyKafkaProducer

MyKafkaProducer(host = "localhost", port = 9092).use { producer ->
    producer.createTopic("reservation-events", partitionCount = 3) // 이미 있으면 ALREADY_EXISTS
    // 단건 (key로 파티션 결정 → 같은 key는 같은 파티션 → 순서 보장)
    val r = producer.produce("reservation-events", key = "seat-A-1", value = """{"status":"RESERVED"}""")
    println("partition=${r.partition} offset=${r.baseOffset}")
    // 배치 (throughput의 핵심)
    producer.produceBatch("reservation-events", listOf(
        "seat-A-2".toByteArray() to """{"status":"RESERVED"}""".toByteArray(),
    ), partition = -1)
}
```

### Consumer (offset 직접 commit = at-least-once)
```kotlin
import com.example.mykafka.client.MyKafkaConsumer

MyKafkaConsumer(host = "localhost", port = 9092, group = "my-group").use { consumer ->
    // (선택) consumer group 자동 파티션 할당
    val assignment = consumer.joinGroup("reservation-events")
    for (partition in assignment.partitions) {
        val committed = consumer.fetchOffset("reservation-events", partition)
        val start = if (committed < 0) 0L else committed
        val result = consumer.fetch("reservation-events", partition, start)
        for (record in result.records) {
            println(String(record.value)) // ... 처리 (멱등하게)
        }
        // 처리 성공 후에만 commit → at-least-once
        consumer.commitOffset("reservation-events", partition, result.nextOffset)
    }
}
```

### 브로커 실행
```bash
# 이 레포에서
./gradlew :broker:run --args="9092 data"
#   args: [port] [dataDir] [maxSegmentBytes] [indexIntervalBytes]
# env:
#   MYKAFKA_FETCH_ZEROCOPY=true   # FETCH를 zero-copy(transferTo)로 (기본 false=heap)
```

## 구현한 것 / 안 한 것

**구현**: append-only Log + segment 롤링, sparse mmap offset index, 파티션·key 파티셔너,
batch produce/fetch, `__consumer_offsets` 내부 토픽 offset 영속화, consumer group 자동 할당(round-robin/lease),
zero-copy FETCH(옵션), 클라이언트 재연결. 자세한 설계는 [`DESIGN.md`](DESIGN.md).

**의도적으로 안 한 것**: replication/ISR, KRaft 컨트롤러, log compaction, exactly-once,
sticky/cooperative rebalance, 비동기 클라이언트.

## 라이선스
MIT. 학습/포트폴리오 용도로 자유롭게.
