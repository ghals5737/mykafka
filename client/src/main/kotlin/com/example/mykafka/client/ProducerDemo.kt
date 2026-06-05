package com.example.mykafka.client

// Producer 사용 예시 + 가벼운 throughput 측정.
//
// 실행 전:
//   broker 띄우기:  ./gradlew run --args="9092 data"   (별 터미널)
// 그 다음:
//   ./gradlew producerDemo
//   또는 IDE 에서 main() 실행
//
// 무엇을 보여주나
//   1) CREATE_TOPIC      — 토픽 만들기 (이미 있으면 ALREADY_EXISTS)
//   2) PRODUCE 단건 3번  — offset 0,1,2 가 부여되는지
//   3) PRODUCE batch 1000건 — 한 번에 1000 record. baseOffset + count 응답.
//   4) 1000건 단건 vs 1×1000 batch 시간 비교 (61x 검증)
fun main() {
    MyKafkaProducer().use { producer ->
        // 1. 토픽 생성 (이미 있으면 OK)
        val topic = "reservation-events"
        val create = producer.createTopic(topic, partitionCount = 3)
        println("[1] CREATE_TOPIC topic=$topic partitions=3 → $create")

        // 2. 단건 PRODUCE 3개 — key 기반 sticky partition 효과 확인
        println()
        println("[2] PRODUCE 3 single records (sticky partition by key)")
        repeat(3) { i ->
            val r = producer.produce(topic, key = "seat-A-${i + 1}", value = "RESERVED by user-$i")
            println("    seat-A-${i + 1} → partition=${r.partition} offset=${r.baseOffset}")
        }

        // 3. PRODUCE batch — 1000 record 한 번에
        println()
        println("[3] PRODUCE batch 1000 records (single call)")
        val batchRecords = (1..1000).map { i ->
            ("seat-batch-$i".toByteArray() as ByteArray?) to "RESERVED-batch-$i".toByteArray()
        }
        val batchStart = System.nanoTime()
        val batchResult = producer.produceBatch(topic, batchRecords, partition = 0)
        val batchMs = (System.nanoTime() - batchStart) / 1_000_000.0
        println("    baseOffset=${batchResult.baseOffset} count=${batchResult.count} → ${"%.2f".format(batchMs)} ms")

        // 4. 1000건을 단건씩 PRODUCE — batch 대비 얼마나 느린지
        println()
        println("[4] PRODUCE 1000 single records (one by one)")
        val singleStart = System.nanoTime()
        repeat(1000) { i ->
            producer.produce(topic, key = "seat-single-$i", value = "RESERVED-single-$i", partition = 1)
        }
        val singleMs = (System.nanoTime() - singleStart) / 1_000_000.0
        println("    1000 single PRODUCEs → ${"%.2f".format(singleMs)} ms")

        // 비교
        println()
        println("─── BATCH vs SINGLE ───")
        println("    batch  (1×1000) : ${"%.2f".format(batchMs)} ms")
        println("    single (1000×1) : ${"%.2f".format(singleMs)} ms")
        println("    speedup         : ${"%.1f".format(singleMs / batchMs)}x  (broker MVP 검증값: ~61x)")
    }
}
