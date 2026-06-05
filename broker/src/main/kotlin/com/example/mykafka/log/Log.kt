package com.example.mykafka.log

import com.example.mykafka.protocol.Record
import com.example.mykafka.protocol.RecordCodec

import org.slf4j.LoggerFactory
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.io.path.name

// 한 파티션을 책임지는 append-only Log.
// 내부에 다수의 Segment를 들고 있고, 항상 하나만 "active(쓰기 대상)" 이다.
// 각 Segment는 자기 .log + .index를 가진다.
class Log(
    private val dir: Path,
    private val maxSegmentBytes: Long = DEFAULT_MAX_SEGMENT_BYTES,
    private val indexIntervalBytes: Int = DEFAULT_INDEX_INTERVAL_BYTES,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val lock = ReentrantLock()

    private val segments: MutableList<Segment> = mutableListOf()

    @Volatile private var nextOffset: Long = 0

    // segment 하나당 최대 인덱스 엔트리 수. 약간 여유.
    private val indexMaxEntries: Int =
        ((maxSegmentBytes / indexIntervalBytes) + 16).toInt()

    init {
        Files.createDirectories(dir)
        loadSegments()
        if (segments.isEmpty()) {
            segments.add(newSegment(baseOffset = 0))
        }
        recoverAll()
        log.info(
            "Log opened dir={} segments={} active={} nextOffset={} indexIntervalBytes={}",
            dir, segments.size, activeSegment().logPath.name, nextOffset, indexIntervalBytes,
        )
    }

    fun nextOffset(): Long = nextOffset

    fun append(key: ByteArray?, value: ByteArray, timestamp: Long = System.currentTimeMillis()): Long =
        lock.withLock {
            maybeRoll()
            val offset = nextOffset
            val bytes = RecordCodec.encode(offset, timestamp, key, value)
            val seg = activeSegment()
            seg.append(offset, bytes)
            seg.nextOffset = offset + 1
            nextOffset = offset + 1
            offset
        }

    // 배치 append. 반환: 첫 record의 offset (baseOffset). 모두 같은 segment에 들어간다.
    //
    // 왜 batch 단위 throughput이 중요한가?
    //   - 네트워크 RTT, 락 획득/해제, syscall 비용은 record 수가 아니라 batch 수에 비례한다.
    //   - 100건짜리 batch 1개 vs 1건씩 100번 → 동일한 디스크 바이트인데도 후자가 수십~수백배 느리다.
    //   - Kafka가 "수십만 msgs/sec" 같은 숫자를 내는 핵심 이유.
    fun appendBatch(records: List<Pair<ByteArray?, ByteArray>>, timestamp: Long = System.currentTimeMillis()): Long =
        lock.withLock {
            require(records.isNotEmpty()) { "empty batch" }
            maybeRoll()
            val baseOffset = nextOffset
            val recordBytes = records.mapIndexed { i, (k, v) ->
                RecordCodec.encode(baseOffset + i, timestamp, k, v)
            }
            val seg = activeSegment()
            seg.appendBatch(baseOffset, recordBytes)
            seg.nextOffset = baseOffset + records.size
            nextOffset = baseOffset + records.size
            baseOffset
        }

    // 특정 offset의 record를 인덱스 활용해 찾는다.
    //   1) segments는 baseOffset 오름차순 정렬 → 이진 탐색으로 해당 segment 결정
    //   2) Segment.findRecord에 위임 (segment 내부 sparse index lookup)
    fun find(offset: Long): Record? {
        if (offset < 0 || offset >= nextOffset) return null
        val segIdx = segmentIndexFor(offset) ?: return null
        return segments[segIdx].findRecord(offset)
    }

    // startOffset 부터 가능한 만큼 record를 모은다 (segment 경계 넘어가며).
    //
    // - 비어 있으면 (startOffset이 끝을 넘었거나 데이터 없음) emptyList.
    // - 누적 byte가 maxBytes를 초과하지 않도록 잘라낸다.
    // - 단, 적어도 1건은 보장 (consumer 진행 보장). 첫 record가 maxBytes보다 커도 포함.
    fun read(startOffset: Long, maxBytes: Int): List<Record> {
        if (startOffset >= nextOffset) return emptyList()
        var idx = segmentIndexFor(startOffset) ?: return emptyList()

        val out = mutableListOf<Record>()
        var bytesUsed = 0
        var off = startOffset
        while (idx <= segments.lastIndex) {
            val budget = if (out.isEmpty()) maxBytes else (maxBytes - bytesUsed)
            if (out.isNotEmpty() && budget <= 0) break
            val (recs, used) = segments[idx].readFrom(off, budget)
            if (recs.isEmpty()) break
            out.addAll(recs)
            bytesUsed += used
            off = recs.last().offset + 1
            idx++
        }
        return out
    }

    // zero-copy FETCH용. read()와 같은 규칙이되 record를 디코드하지 않고
    // "보낼 파일 구간 목록"을 반환한다. segment 경계를 넘으면 region이 여러 개가 된다
    // (각 region은 한 segment 파일 안의 연속 바이트 → transferTo 한 번).
    fun fetchRegions(startOffset: Long, maxBytes: Int): List<FetchRegionRef> {
        if (startOffset >= nextOffset) return emptyList()
        var idx = segmentIndexFor(startOffset) ?: return emptyList()

        val out = mutableListOf<FetchRegionRef>()
        var bytesUsed = 0
        var off = startOffset
        while (idx <= segments.lastIndex) {
            if (out.isNotEmpty() && bytesUsed >= maxBytes) break
            val budget = if (out.isEmpty()) maxBytes else (maxBytes - bytesUsed)
            val span = segments[idx].regionFrom(off, budget) ?: break
            out.add(FetchRegionRef(segments[idx].logChannel(), span.position, span.length, span.count))
            bytesUsed += span.length
            off = span.lastOffset + 1
            idx++
        }
        return out
    }

    data class FetchRegionRef(
        val channel: java.nio.channels.FileChannel,
        val position: Long,
        val length: Int,
        val count: Int,
    )

    // 디버그/검증용: 모든 segment 직렬 스캔.
    fun readAll(): List<Record> {
        val out = mutableListOf<Record>()
        for (seg in segments) {
            seg.scan { rec, _ -> out.add(rec) }
        }
        return out
    }

    fun segmentBaseOffsets(): List<Long> = segments.map { it.baseOffset }
    fun indexEntryCounts(): List<Int> = segments.map { it.indexEntryCount() }

    fun close() {
        segments.forEach { it.close() }
    }

    // segment 이진 탐색: baseOffset ≤ target 인 가장 큰 인덱스 반환.
    private fun segmentIndexFor(offset: Long): Int? {
        var lo = 0
        var hi = segments.lastIndex
        var best = -1
        while (lo <= hi) {
            val mid = (lo + hi) ushr 1
            if (segments[mid].baseOffset <= offset) {
                best = mid
                lo = mid + 1
            } else {
                hi = mid - 1
            }
        }
        return if (best < 0) null else best
    }

    private fun activeSegment(): Segment = segments.last()

    private fun maybeRoll() {
        val active = activeSegment()
        if (active.sizeBytes < maxSegmentBytes) return
        val newBase = nextOffset
        segments.add(newSegment(newBase))
        log.info("rolled active segment: new baseOffset={} (prev size={}B)", newBase, active.sizeBytes)
    }

    private fun newSegment(baseOffset: Long): Segment =
        Segment(
            baseOffset = baseOffset,
            logPath = dir.resolve(Segment.logFileName(baseOffset)),
            indexPath = dir.resolve(Segment.indexFileName(baseOffset)),
            indexIntervalBytes = indexIntervalBytes,
            indexMaxEntries = indexMaxEntries,
        )

    private fun loadSegments() {
        Files.list(dir).use { stream ->
            stream
                .filter { it.fileName.toString().endsWith(".log") }
                .map { p ->
                    val base = Segment.parseBaseOffsetFromLog(p.fileName.toString())
                        ?: error("malformed segment file name: ${p.fileName}")
                    newSegment(base)
                }
                .sorted(compareBy { it.baseOffset })
                .forEach { segments.add(it) }
        }
    }

    // 모든 segment에 대해 .log 스캔으로 인덱스 재구축.
    // 마지막(active) segment에 한해 부분 쓰기 꼬리 자르기.
    private fun recoverAll() {
        for ((i, seg) in segments.withIndex()) {
            val isActive = (i == segments.lastIndex)
            seg.rebuildIndex(truncateTail = isActive)
        }
        nextOffset = activeSegment().nextOffset
    }

    companion object {
        // 학습 목적 기본값들. 실 Kafka: 1GB segment, 4KB index interval.
        const val DEFAULT_MAX_SEGMENT_BYTES: Long = 1L * 1024 * 1024
        const val DEFAULT_INDEX_INTERVAL_BYTES: Int = 4 * 1024
    }
}
