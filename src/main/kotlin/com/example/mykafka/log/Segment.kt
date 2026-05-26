package com.example.mykafka.log

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption.CREATE
import java.nio.file.StandardOpenOption.READ
import java.nio.file.StandardOpenOption.WRITE

// 한 개의 .log + .index = Segment.
//
// 파일명:
//   <20자리 zero-pad baseOffset>.log
//   <20자리 zero-pad baseOffset>.index
//
// 인덱스 정책: indexIntervalBytes 마다 1엔트리. sparse.
// 인덱스는 시작 시 .log를 스캔해 재구축한다.
class Segment(
    val baseOffset: Long,
    val logPath: Path,
    indexPath: Path,
    private val indexIntervalBytes: Int,
    indexMaxEntries: Int,
) {
    private val channel: FileChannel = FileChannel.open(logPath, CREATE, READ, WRITE)
    private val index: OffsetIndex = OffsetIndex(indexPath, indexMaxEntries)

    @Volatile var sizeBytes: Long = channel.size()
        private set

    @Volatile var nextOffset: Long = baseOffset
        internal set

    // 가장 최근에 인덱스에 등록한 record의 byte position. -1 = 아직 없음.
    private var lastIndexedPos: Long = -1L

    // 호출자(=Log)가 락을 들고 있다는 전제. record 1건 append + 필요시 인덱스 갱신.
    fun append(offset: Long, bytes: ByteArray) {
        val pos = sizeBytes
        channel.position(pos)
        val buf = ByteBuffer.wrap(bytes)
        while (buf.hasRemaining()) channel.write(buf)
        sizeBytes += bytes.size

        // sparse 인덱스 규칙:
        //   - 첫 record는 무조건 인덱싱 (탐색 시작점 보장)
        //   - 그 이후엔 마지막 인덱싱 위치로부터 indexIntervalBytes 이상 벌어졌을 때만
        if (lastIndexedPos < 0 || pos - lastIndexedPos >= indexIntervalBytes) {
            index.append((offset - baseOffset).toInt(), pos.toInt())
            lastIndexedPos = pos
        }
    }

    // 한 번에 N건을 append. 한 번의 channel.write로 묶고, 인덱스 엔트리는 record별로 갱신.
    //
    // 왜 한 번에 쓰는가?
    //   - syscall 수 = 1로 줄어 throughput 상승. record 사이 page-cache 동기화 비용 없음.
    //   - "batch는 한 원자(atomic) 단위로 디스크에 쓰인다" 라는 의미를 코드 구조로 표현.
    //   - 단, OS 레벨 atomic은 아니다 (전원이 나가면 부분 쓰기 가능). 이는 Step 2의
    //     "꼬리 자르기" 복구로 보완된다.
    fun appendBatch(baseOffsetOfBatch: Long, recordBytes: List<ByteArray>) {
        if (recordBytes.isEmpty()) return
        val startPos = sizeBytes
        val totalLen = recordBytes.sumOf { it.size }
        val combined = ByteBuffer.allocate(totalLen)
        val perRecordPos = LongArray(recordBytes.size)
        var cursor = startPos
        for ((i, b) in recordBytes.withIndex()) {
            perRecordPos[i] = cursor
            combined.put(b)
            cursor += b.size
        }
        combined.flip()
        channel.position(startPos)
        while (combined.hasRemaining()) channel.write(combined)
        sizeBytes += totalLen

        for ((i, pos) in perRecordPos.withIndex()) {
            if (lastIndexedPos < 0 || pos - lastIndexedPos >= indexIntervalBytes) {
                val rel = ((baseOffsetOfBatch + i) - baseOffset).toInt()
                index.append(rel, pos.toInt())
                lastIndexedPos = pos
            }
        }
    }

    // startOffset 이상의 record들을 maxBytes 안에서 모아 반환.
    //
    // 동작
    //   1) 인덱스로 startOffset 이하의 가장 큰 엔트리 lookup → 그 byte position부터 decode
    //   2) startOffset보다 작은 record는 skip (sparse index 특성상 약간 앞에서 시작)
    //   3) record 누적: 첫 record는 무조건 포함 (≥maxBytes여도 보냄 — consumer 진행 보장),
    //      이후엔 누적 byte가 maxBytes 초과하지 않을 때까지
    //
    // 반환: (records, totalBytesUsed)
    //
    // 왜 "첫 record는 maxBytes 무시"인가?
    //   - record가 maxBytes보다 크면 영원히 못 보낸다 → consumer가 진행 불가.
    //   - Kafka도 동일한 invariant: "at least one record must be returnable".
    fun readFrom(startOffset: Long, maxBytes: Int): Pair<List<Record>, Int> {
        if (startOffset < baseOffset || startOffset >= nextOffset) return emptyList<Record>() to 0
        val rel = (startOffset - baseOffset).toInt()
        val (_, startPos) = index.lookup(rel).let { it[0] to it[1] }
        val len = (sizeBytes - startPos).toInt()
        if (len <= 0) return emptyList<Record>() to 0

        val arr = ByteArray(len)
        val buf = ByteBuffer.wrap(arr)
        channel.position(startPos.toLong())
        while (buf.hasRemaining()) {
            if (channel.read(buf) < 0) break
        }
        buf.flip()

        val out = mutableListOf<Record>()
        var used = 0
        while (buf.hasRemaining()) {
            val before = buf.position()
            val rec = RecordCodec.decode(buf) ?: break
            if (rec.offset < startOffset) continue
            val recSize = buf.position() - before
            if (out.isNotEmpty() && used + recSize > maxBytes) break
            out.add(rec)
            used += recSize
        }
        return out to used
    }

    // 인덱스를 활용해 특정 offset의 record를 찾는다.
    //   1) 인덱스에서 target 이하의 가장 큰 엔트리 lookup
    //   2) 그 byte position부터 sequential decode
    //   3) 원하는 offset의 record가 나오면 반환, 지나가면 null
    fun findRecord(offset: Long): Record? {
        if (offset < baseOffset || offset >= nextOffset) return null
        val rel = (offset - baseOffset).toInt()
        val (_, startPos) = index.lookup(rel).let { it[0] to it[1] }

        // startPos부터 sizeBytes 까지 읽어 decode
        val len = (sizeBytes - startPos).toInt()
        if (len <= 0) return null
        val arr = ByteArray(len)
        val buf = ByteBuffer.wrap(arr)
        channel.position(startPos.toLong())
        while (buf.hasRemaining()) {
            if (channel.read(buf) < 0) break
        }
        buf.flip()

        while (buf.hasRemaining()) {
            val rec = RecordCodec.decode(buf) ?: return null
            if (rec.offset == offset) return rec
            if (rec.offset > offset) return null // 지나감
        }
        return null
    }

    // 전체 스캔. 디버그/recovery용.
    // onRecord: (record, recordStartPos) → unit
    // 반환: 끝까지 정상이면 sizeBytes, 깨진 지점이 있으면 그 직전 byte 위치.
    fun scan(onRecord: (Record, Long) -> Unit): Long {
        if (sizeBytes == 0L) return 0
        val arr = ByteArray(sizeBytes.toInt())
        val buf = ByteBuffer.wrap(arr)
        channel.position(0)
        while (buf.hasRemaining()) {
            if (channel.read(buf) < 0) break
        }
        buf.flip()

        var lastGoodPos = 0
        while (buf.hasRemaining()) {
            val before = buf.position()
            val rec = try {
                RecordCodec.decode(buf)
            } catch (e: Exception) {
                buf.position(before); break
            } ?: break
            onRecord(rec, before.toLong())
            lastGoodPos = buf.position()
        }
        return lastGoodPos.toLong()
    }

    // 시작 시 .log를 처음부터 스캔해 인덱스를 재구축 + nextOffset/sizeBytes 결정.
    // 마지막에 부분 쓰기된 trailing bytes가 있으면 그 만큼 truncate.
    // 반환: 마지막 정상 byte position.
    fun rebuildIndex(truncateTail: Boolean): Long {
        index.reset()
        lastIndexedPos = -1L
        var lastOffset = -1L
        val goodPos = scan { rec, startPos ->
            if (lastIndexedPos < 0 || startPos - lastIndexedPos >= indexIntervalBytes) {
                index.append((rec.offset - baseOffset).toInt(), startPos.toInt())
                lastIndexedPos = startPos
            }
            lastOffset = rec.offset
        }
        if (truncateTail && goodPos != sizeBytes) {
            channel.truncate(goodPos)
            sizeBytes = goodPos
        }
        nextOffset = if (lastOffset < 0) baseOffset else lastOffset + 1
        return goodPos
    }

    fun indexEntryCount(): Int = index.entryCount

    fun close() {
        index.close()
        channel.close()
    }

    companion object {
        fun logFileName(baseOffset: Long): String = "%020d.log".format(baseOffset)
        fun indexFileName(baseOffset: Long): String = "%020d.index".format(baseOffset)

        fun parseBaseOffsetFromLog(fileName: String): Long? {
            if (!fileName.endsWith(".log")) return null
            return fileName.removeSuffix(".log").toLongOrNull()
        }
    }
}
