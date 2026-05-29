package com.example.mykafka.client

// FETCH 처리량 벤치 — zero-copy(transferTo) vs heap 복사 경로를 격리 측정.
//
// 앱 이벤트(~150B)는 너무 작아 zero-copy 효과가 안 보인다. 여기선 큰 value로
// "디스크→소켓 대량 전송"을 만들어 fetch 경로 비용만 측정한다.
//
// 동작: topic 생성 → RECORD_COUNT개(VALUE_SIZE bytes) produce → 5초간 offset 0부터
//   maxBytes씩 끝까지 반복 fetch하며 받은 바이트/record 수 집계 → MB/s, rec/s 출력.
//
// 비교: broker를 MYKAFKA_FETCH_ZEROCOPY=true/false로 각각 띄우고 (fresh data dir) 이 데모 실행.
//
// env: VALUE_SIZE(기본 1024), RECORD_COUNT(기본 50000), MAX_BYTES(기본 1MB), BENCH_MS(기본 5000)
fun main() {
    val valueSize = System.getenv("VALUE_SIZE")?.toIntOrNull() ?: 1024
    val recordCount = System.getenv("RECORD_COUNT")?.toIntOrNull() ?: 50_000
    val maxBytes = System.getenv("MAX_BYTES")?.toIntOrNull() ?: (1 shl 20)
    val benchMs = System.getenv("BENCH_MS")?.toLongOrNull() ?: 5_000L
    val topic = "fetch-bench"

    MyKafkaProducer().use { producer ->
        producer.createTopic(topic, 1)
        // produce: VALUE_SIZE 바이트 value를 1000개씩 batch로
        val value = ByteArray(valueSize) { ('a' + (it % 26)).code.toByte() }
        var produced = 0
        val chunk = 1000
        while (produced < recordCount) {
            val n = minOf(chunk, recordCount - produced)
            val batch = (0 until n).map { (null as ByteArray?) to value }
            producer.produceBatch(topic, batch, partition = 0)
            produced += n
        }
        println("[produce] $recordCount records × $valueSize B = ${recordCount.toLong() * valueSize / 1_000_000} MB")
    }

    MyKafkaConsumer(group = "fetch-bench-g").use { consumer ->
        // 워밍업 한 바퀴
        run {
            var off = 0L
            while (true) { val r = consumer.fetch(topic, 0, off, maxBytes); if (r.records.isEmpty()) break; off = r.nextOffset }
        }
        var totalRecords = 0L
        var totalBytes = 0L
        var fetchCalls = 0L
        val start = System.nanoTime()
        val deadline = start + benchMs * 1_000_000
        var off = 0L
        while (System.nanoTime() < deadline) {
            val r = consumer.fetch(topic, 0, off, maxBytes)
            fetchCalls++
            if (r.records.isEmpty()) { off = 0L; continue } // 끝 → 처음부터 다시
            totalRecords += r.records.size
            for (rec in r.records) totalBytes += rec.value.size
            off = r.nextOffset
        }
        val elapsedS = (System.nanoTime() - start) / 1e9
        val mbps = totalBytes / 1e6 / elapsedS
        val recps = totalRecords / elapsedS
        println("─── FETCH bench (${"%.1f".format(elapsedS)}s) ───")
        println("    fetch calls   : $fetchCalls")
        println("    records       : $totalRecords  (${"%.0f".format(recps)}/s)")
        println("    value bytes   : ${"%.1f".format(totalBytes / 1e6)} MB  → ${"%.1f".format(mbps)} MB/s")
    }
}
