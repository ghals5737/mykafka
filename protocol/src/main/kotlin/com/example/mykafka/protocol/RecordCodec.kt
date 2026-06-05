package com.example.mykafka.protocol

import java.nio.ByteBuffer
import java.util.zip.CRC32

// Record ↔ bytes 변환.
//
// 헤더 = offset(8) + timestamp(8) + keyLen(4)
// 가변 = key(keyLen 또는 0) + valueLen(4) + value(valueLen)
// 꼬리 = crc(4)
//
// totalSize는 호출자가 알 수 있어야 하므로 encode가 byte[]를 통째 반환한다.
object RecordCodec {

    private const val HEADER_FIXED = 8 + 8 + 4 // offset + timestamp + keyLen
    private const val VALUE_LEN = 4
    private const val CRC_LEN = 4

    fun encodedSize(key: ByteArray?, value: ByteArray): Int {
        val keyBytes = key?.size ?: 0
        return HEADER_FIXED + keyBytes + VALUE_LEN + value.size + CRC_LEN
    }

    fun encode(offset: Long, timestamp: Long, key: ByteArray?, value: ByteArray): ByteArray {
        val size = encodedSize(key, value)
        val buf = ByteBuffer.allocate(size)
        buf.putLong(offset)
        buf.putLong(timestamp)
        if (key == null) {
            buf.putInt(-1)
        } else {
            buf.putInt(key.size)
            buf.put(key)
        }
        buf.putInt(value.size)
        buf.put(value)

        // CRC: 지금까지 쓴 모든 바이트(헤더+페이로드)에 대해 계산
        val crc = CRC32()
        crc.update(buf.array(), 0, buf.position())
        buf.putInt(crc.value.toInt())
        return buf.array()
    }

    // 주어진 ByteBuffer에서 record 하나를 읽어낸다.
    // 데이터가 부족하면 null 반환 (호출자가 truncated tail로 판단).
    // CRC 불일치는 예외.
    fun decode(buf: ByteBuffer): Record? {
        val start = buf.position()
        if (buf.remaining() < HEADER_FIXED) return null

        val offset = buf.getLong()
        val timestamp = buf.getLong()
        val keyLen = buf.getInt()

        val key: ByteArray? = if (keyLen == -1) {
            null
        } else {
            if (keyLen < 0) error("invalid keyLen=$keyLen at pos=$start")
            if (buf.remaining() < keyLen) {
                buf.position(start); return null
            }
            ByteArray(keyLen).also { buf.get(it) }
        }

        if (buf.remaining() < VALUE_LEN) {
            buf.position(start); return null
        }
        val valueLen = buf.getInt()
        if (valueLen < 0) error("invalid valueLen=$valueLen at pos=$start")
        if (buf.remaining() < valueLen + CRC_LEN) {
            buf.position(start); return null
        }
        val value = ByteArray(valueLen).also { buf.get(it) }

        val expectedCrc = buf.getInt()
        // CRC: start..(end of value) 바이트들에 대해 계산해서 비교
        val recordBytes = buf.array()
        val recordOffset = buf.arrayOffset() + start
        val payloadLen = (buf.position() - CRC_LEN) - start
        val crc = CRC32()
        crc.update(recordBytes, recordOffset, payloadLen)
        if (crc.value.toInt() != expectedCrc) {
            error("CRC mismatch at offset=$offset (corruption suspected)")
        }

        return Record(offset, timestamp, key, value)
    }
}
