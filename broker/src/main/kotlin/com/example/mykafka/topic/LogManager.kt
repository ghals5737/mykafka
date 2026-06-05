package com.example.mykafka.topic

import com.example.mykafka.log.Log
import org.slf4j.LoggerFactory
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

// 한 브로커가 책임지는 모든 토픽 + 파티션 + Log 인스턴스를 관리.
//
// 디렉토리 레이아웃:
//   <rootDir>/
//     <topicName>/
//       0/  ← 파티션 0
//         00000000000000000000.log
//         00000000000000000000.index
//       1/  ← 파티션 1
//         ...
//
// 왜 토픽/파티션 디렉토리를 그대로 쓰는가?
//   - 메타데이터 파일 없이 디렉토리 자체가 토픽 목록 + 파티션 수 + segment 목록의 단일 출처.
//   - 실 Kafka도 거의 동일한 레이아웃을 쓴다 (topic-partition 한 단계 디렉토리지만 의미는 같음).
//
// 왜 파티션마다 별도 Log 인스턴스인가?
//   - 락은 Log.append 내부에만 → 파티션 간 락 충돌 없음.
//   - 파티션이 곧 "병렬성의 단위" 라는 Kafka 정체성을 코드로 그대로 표현.
class LogManager(
    private val rootDir: Path,
    private val maxSegmentBytes: Long,
    private val indexIntervalBytes: Int,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val topics = ConcurrentHashMap<String, Topic>()

    init {
        Files.createDirectories(rootDir)
        loadExisting()
        log.info("LogManager opened rootDir={} topics={}", rootDir, topics.keys)
    }

    fun createTopic(name: String, partitionCount: Int): CreateResult {
        if (!isValidTopicName(name)) return CreateResult.INVALID
        if (partitionCount <= 0) return CreateResult.INVALID
        if (topics.containsKey(name)) return CreateResult.ALREADY_EXISTS

        val topicDir = rootDir.resolve(name)
        Files.createDirectories(topicDir)
        val partitions = (0 until partitionCount).map { pid ->
            val pdir = topicDir.resolve(pid.toString())
            Files.createDirectories(pdir)
            Log(pdir, maxSegmentBytes, indexIntervalBytes)
        }
        topics[name] = Topic(name, partitions)
        log.info("created topic={} partitions={}", name, partitionCount)
        return CreateResult.OK
    }

    fun getPartition(topic: String, partition: Int): Log? {
        val t = topics[topic] ?: return null
        return t.partitions.getOrNull(partition)
    }

    // 파티션 선택(클라이언트가 partition=-1 줬을 때).
    //   - key 있음: hash(key) % partitionCount  → 같은 key는 같은 파티션
    //   - key 없음: round-robin
    //
    // 왜 "같은 key는 같은 파티션"인가?
    //   - 파티션 내에서만 순서가 보장된다.
    //   - 같은 entity(예: 같은 user_id)의 이벤트들이 다른 파티션으로 흩어지면
    //     순서가 보장되지 않는다. key로 한 곳에 묶어야 의미 있는 순서를 가진다.
    //   - 이게 Kafka가 "전역 순서 X, 파티션 순서 O" 라는 trade-off를 받아들인 이유.
    fun pickPartition(topic: String, key: ByteArray?): Int? {
        val t = topics[topic] ?: return null
        val n = t.partitions.size
        return if (key == null) t.nextRoundRobin(n)
        else Math.floorMod(key.contentHashCode(), n)
    }

    fun describeTopics(): Map<String, Int> =
        topics.mapValues { it.value.partitions.size }

    fun close() {
        topics.values.forEach { t -> t.partitions.forEach { it.close() } }
    }

    private fun loadExisting() {
        if (!Files.isDirectory(rootDir)) return
        Files.list(rootDir).use { topicsStream ->
            topicsStream
                .filter { Files.isDirectory(it) }
                .filter { isValidTopicName(it.fileName.toString()) }
                .forEach { topicDir ->
                    val name = topicDir.fileName.toString()
                    val partDirs = Files.list(topicDir).use { ps ->
                        ps.filter { Files.isDirectory(it) }
                            .filter { it.fileName.toString().toIntOrNull() != null }
                            .sorted(compareBy { it.fileName.toString().toInt() })
                            .toList()
                    }
                    if (partDirs.isEmpty()) return@forEach
                    // 파티션 id가 0..n-1 연속이어야 한다는 단순한 가정 (학습용)
                    val ids = partDirs.map { it.fileName.toString().toInt() }
                    if (ids != (0 until ids.size).toList()) {
                        log.warn("skipping topic={} — non-contiguous partition ids: {}", name, ids)
                        return@forEach
                    }
                    val partitions = partDirs.map { Log(it, maxSegmentBytes, indexIntervalBytes) }
                    topics[name] = Topic(name, partitions)
                    log.info("loaded topic={} partitions={}", name, partitions.size)
                }
        }
    }

    enum class CreateResult { OK, ALREADY_EXISTS, INVALID }

    private class Topic(val name: String, val partitions: List<Log>) {
        private val rr = AtomicInteger(0)
        fun nextRoundRobin(n: Int): Int = Math.floorMod(rr.getAndIncrement(), n)
    }

    companion object {
        private val NAME_REGEX = Regex("^[A-Za-z0-9_-]{1,64}$")
        fun isValidTopicName(name: String): Boolean = NAME_REGEX.matches(name)
    }
}
