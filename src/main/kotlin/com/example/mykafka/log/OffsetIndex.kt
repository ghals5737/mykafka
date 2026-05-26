package com.example.mykafka.log

import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption.CREATE
import java.nio.file.StandardOpenOption.READ
import java.nio.file.StandardOpenOption.WRITE

// Segment 하나에 딸린 sparse offset index.
//
// 엔트리 포맷 (8바이트): [relativeOffset:4][filePosition:4]
//   - relativeOffset = recordOffset - segment.baseOffset
//   - 4바이트 Int면 segment 안에 최대 2^31 record까지 표현 가능 → 충분
//   - filePosition = .log 파일 내 byte 위치
//
// 왜 sparse(듬성듬성)인가?
//   - 모든 record마다 인덱스 만들면 인덱스 크기 = record 수에 비례 → 메모리 부담.
//   - 대신 "indexIntervalBytes 마다 1엔트리"로 듬성듬성 둔다.
//   - lookup(offset) = "그 offset보다 작거나 같은 가장 큰 인덱스 엔트리" → 거기서부터 짧은 sequential scan.
//   - sequential scan은 매우 빠르므로 sparse + 짧은 스캔이 dense 인덱스보다 효율적.
//
// 왜 mmap인가?
//   - 인덱스는 작고 자주 읽힌다 → OS 페이지 캐시에 위임하는 게 가장 효율.
//   - read/write syscall 안 거치고 메모리처럼 접근.
//
// 인덱스 파일 사이즈는 시작 시 pre-allocate. 실제 유효 엔트리 수는 entryCount로 관리.
//   - 시작 시 항상 .log를 스캔해 재구축 (인덱스가 망가져도 로그만 멀쩡하면 복구 가능).
class OffsetIndex(
    val path: Path,
    val maxEntries: Int,
) {
    private val channel: FileChannel
    private val mmap: MappedByteBuffer

    @Volatile var entryCount: Int = 0
        private set

    init {
        val fileSize = (maxEntries.toLong() * ENTRY_SIZE)
        channel = FileChannel.open(path, CREATE, READ, WRITE)
        if (channel.size() < fileSize) channel.truncate(fileSize)
        mmap = channel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize)
    }

    fun append(relativeOffset: Int, filePosition: Int) {
        check(entryCount < maxEntries) { "index full ($entryCount / $maxEntries)" }
        val off = entryCount * ENTRY_SIZE
        mmap.putInt(off, relativeOffset)
        mmap.putInt(off + 4, filePosition)
        entryCount += 1
    }

    fun reset() {
        entryCount = 0
    }

    // 주어진 relativeOffset 이하 중 가장 큰 엔트리를 이진 탐색.
    // 반환: 그 엔트리의 (relativeOffset, filePosition).
    //   인덱스에 엔트리가 없으면 (0, 0).
    fun lookup(targetRelativeOffset: Int): IntArray {
        if (entryCount == 0) return intArrayOf(0, 0)
        var lo = 0
        var hi = entryCount - 1
        var bestIdx = -1
        while (lo <= hi) {
            val mid = (lo + hi) ushr 1
            val midOff = mmap.getInt(mid * ENTRY_SIZE)
            if (midOff <= targetRelativeOffset) {
                bestIdx = mid
                lo = mid + 1
            } else {
                hi = mid - 1
            }
        }
        if (bestIdx < 0) return intArrayOf(0, 0)
        val rel = mmap.getInt(bestIdx * ENTRY_SIZE)
        val pos = mmap.getInt(bestIdx * ENTRY_SIZE + 4)
        return intArrayOf(rel, pos)
    }

    fun close() {
        // mmap.force()로 dirty 페이지 flush. OS가 잘 해주지만 명시.
        mmap.force()
        channel.close()
    }

    companion object {
        const val ENTRY_SIZE = 8
    }
}
