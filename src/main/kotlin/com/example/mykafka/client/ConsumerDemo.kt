package com.example.mykafka.client

// Consumer 사용 예시 + Kafka "재시작 이어가기" 검증.
//
// 실행 전:
//   1) broker 띄우기:  ./gradlew run --args="9092 data"
//   2) (선택) ProducerDemo 먼저 실행해서 reservation-events 토픽에 시드.
//      이미 있으면 그대로 사용.
//
// 실행:
//   ./gradlew consumerDemo
//
// 무엇을 보여주나
//   [1] fetchOffset — 미커밋 group 이면 -1 반환
//   [2] fetch       — partition=0 처음부터 가져오기. record 가 self-delimiting 으로 decode 됨
//   [3] commitOffset — "여기까지 처리했어요" 기록
//   [4] fetchOffset — commit 값 그대로 돌아옴
//   [5] "재시작 시뮬레이션" — 새 Consumer 인스턴스로 poll → 자동으로 commit 직후부터
fun main() {
    val topic = "reservation-events"
    val partition = 0
    val group = "demo-consumer-group"

    // [1] 새 group 의 첫 조회 → 미커밋이므로 -1
    println("[1] fetchOffset (새 group)")
    MyKafkaConsumer(group = group).use { c ->
        val o = c.fetchOffset(topic, partition)
        println("    group=$group topic=$topic p=$partition → committed offset = $o   (-1 = 미커밋)")
    }

    // [2] 처음부터 fetch
    println()
    println("[2] FETCH from offset=0, maxBytes=4096")
    val fetchResult = MyKafkaConsumer(group = group).use { c ->
        val r = c.fetch(topic, partition, offset = 0, maxBytes = 4096)
        println("    received ${r.records.size} records, nextOffset=${r.nextOffset}")
        r.records.take(5).forEach { rec ->
            val k = rec.key?.toString(Charsets.UTF_8) ?: "<null>"
            val v = rec.value.toString(Charsets.UTF_8)
            println("    offset=${rec.offset}  key=$k  value=$v")
        }
        if (r.records.size > 5) println("    ... (총 ${r.records.size}건 중 처음 5건만 표시)")
        r
    }

    // [3] 처리 완료 → commit (= 받은 record 의 last offset + 1)
    println()
    println("[3] COMMIT_OFFSET offset=${fetchResult.nextOffset}")
    MyKafkaConsumer(group = group).use { c ->
        c.commitOffset(topic, partition, fetchResult.nextOffset)
        println("    OK")
    }

    // [4] 다시 fetchOffset → 방금 commit 한 값
    println()
    println("[4] fetchOffset 재조회")
    MyKafkaConsumer(group = group).use { c ->
        val o = c.fetchOffset(topic, partition)
        println("    committed offset = $o   (방금 commit 한 ${fetchResult.nextOffset} 과 같으면 정상)")
    }

    // [5] "재시작 시뮬레이션" — 새 Consumer 가 자동으로 commit 위치부터 poll
    println()
    println("[5] 재시작 시뮬레이션 — 새 Consumer.poll()")
    MyKafkaConsumer(group = group).use { c ->
        val r = c.poll(topic, partition, maxBytes = 4096)
        if (r.records.isEmpty()) {
            println("    receive 0 records — commit 이후로 새 데이터 없음. (정상: 같은 데이터 다시 안 받음)")
        } else {
            println("    receive ${r.records.size} records, first offset=${r.records.first().offset}")
            println("    → 이전 commit 후로 새로 들어온 데이터")
        }
        println("    nextOffset = ${r.nextOffset}")
    }

    println()
    println("─── 핵심 ───")
    println("    consumer 가 commit 한 시점부터 broker 가 기억해 줌 (__consumer_offsets 토픽에 영속).")
    println("    재시작해도 같은 group 이면 자동으로 이어 받음.")
    println("    auto-commit X — 처리 (DB INSERT 등) 성공 후 명시적 commit 해야 at-least-once 보장.")
}
