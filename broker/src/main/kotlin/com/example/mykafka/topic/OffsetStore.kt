package com.example.mykafka.topic

import org.slf4j.LoggerFactory
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentHashMap

// 컨슈머 group이 어디까지 처리했는지(=offset)를 저장.
//
// 저장 방식: 내부 토픽 "__consumer_offsets" (단일 파티션) 에 append-only.
//   record key   = encode(group, topic, partition)
//   record value = offset (8B big-endian)
//
// 왜 "또 다른 로그"에 저장하는가?
//   - 별도 메타데이터 저장소 없이, broker가 이미 가진 메커니즘을 그대로 재활용.
//   - durability 보장이 데이터 토픽과 동일 (append-only + recovery).
//   - "모든 게 로그" 라는 Kafka 철학의 일관성 — broker가 자기 자신을 dogfooding.
//   - 같은 key가 반복 append 되더라도 가장 최신 record만 의미가 있다 →
//     log compaction을 도입하면 자연스럽게 그 값만 남는다. (compaction은 MVP 범위 밖)
//
// 런타임 lookup:
//   - 시작 시 내부 토픽을 처음부터 끝까지 스캔 → in-memory cache 재구축
//   - COMMIT 시: append + cache 업데이트
//   - FETCH_OFFSET 시: cache 조회 (read 경로는 disk 접근 0)
class OffsetStore(private val logManager: LogManager) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val cache = ConcurrentHashMap<Key, Long>()

    init {
        // 내부 토픽 보장 (이미 있어도 OK)
        val result = logManager.createTopic(INTERNAL_TOPIC, 1)
        check(result != LogManager.CreateResult.INVALID) {
            "internal topic name rejected: $INTERNAL_TOPIC"
        }
        // 시작 시 스캔으로 cache 재구축
        val partition = logManager.getPartition(INTERNAL_TOPIC, 0)
            ?: error("internal topic partition missing")
        var count = 0
        for (rec in partition.readAll()) {
            val k = decodeKey(rec.key ?: continue)
            val v = ByteBuffer.wrap(rec.value).long
            cache[k] = v
            count += 1
        }
        log.info("OffsetStore loaded {} commits ({} unique keys)", count, cache.size)
    }

    fun commit(group: String, topic: String, partition: Int, offset: Long) {
        val keyBytes = encodeKey(group, topic, partition)
        val valueBytes = ByteBuffer.allocate(8).putLong(offset).array()
        val log = logManager.getPartition(INTERNAL_TOPIC, 0)!!
        log.append(keyBytes, valueBytes)
        cache[Key(group, topic, partition)] = offset
    }

    // 미커밋이면 -1 반환.
    fun fetch(group: String, topic: String, partition: Int): Long =
        cache[Key(group, topic, partition)] ?: -1L

    private data class Key(val group: String, val topic: String, val partition: Int)

    // key format: [groupLen:2][group][topicLen:2][topic][partition:4]
    // 같은 (g,t,p)는 항상 같은 byte → log compaction이 의미 있게 동작
    private fun encodeKey(group: String, topic: String, partition: Int): ByteArray {
        val gb = group.toByteArray(StandardCharsets.UTF_8)
        val tb = topic.toByteArray(StandardCharsets.UTF_8)
        val buf = ByteBuffer.allocate(2 + gb.size + 2 + tb.size + 4)
        buf.putShort(gb.size.toShort()); buf.put(gb)
        buf.putShort(tb.size.toShort()); buf.put(tb)
        buf.putInt(partition)
        return buf.array()
    }

    private fun decodeKey(bytes: ByteArray): Key {
        val buf = ByteBuffer.wrap(bytes)
        val gl = buf.short.toInt(); val gb = ByteArray(gl).also { buf.get(it) }
        val tl = buf.short.toInt(); val tb = ByteArray(tl).also { buf.get(it) }
        val p = buf.int
        return Key(String(gb, StandardCharsets.UTF_8), String(tb, StandardCharsets.UTF_8), p)
    }

    companion object {
        const val INTERNAL_TOPIC = "__consumer_offsets"
    }
}
