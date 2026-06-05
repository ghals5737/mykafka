package com.example.mykafka.protocol

// 한 메시지 = 한 Record.
//
// 디스크에 기록되는 포맷:
//   [offset:8][timestamp:8][keyLen:4][key:keyLen][valueLen:4][value:valueLen][crc:4]
//
// - keyLen == -1  → key 없음 (null)
// - crc          → offset 시작부터 value 끝까지 바이트 전체에 대한 CRC32
//
// 왜 offset을 record 안에 직접 박는가?
//   1) offset이 곧 메시지의 정체성 = 위치이자 ID. 별도 ID 생성기가 불필요.
//   2) 추후 복구 시 파일을 처음부터 스캔하면 마지막 offset을 그대로 알 수 있다.
//   3) 인덱스가 망가져도 로그만 있으면 모든 정보 복원 가능. (자기 완결성)
//
// 왜 timestamp까지 박는가?
//   - 시간 기반 retention/검색 (예: "지난 24시간 메시지만 컨슈머가 처리")
//   - Kafka도 정확히 같은 자리에 timestamp를 둔다.
//
// 왜 CRC인가?
//   - 디스크/네트워크 비트 손상을 record 단위로 즉시 감지.
//   - "한 record가 부분적으로만 쓰여 있다(찢어진 write)" 같은 상황도 잡힌다.
data class Record(
    val offset: Long,
    val timestamp: Long,
    val key: ByteArray?,
    val value: ByteArray,
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Record) return false
        return offset == other.offset &&
            timestamp == other.timestamp &&
            (key?.contentEquals(other.key) ?: (other.key == null)) &&
            value.contentEquals(other.value)
    }

    override fun hashCode(): Int {
        var result = offset.hashCode()
        result = 31 * result + timestamp.hashCode()
        result = 31 * result + (key?.contentHashCode() ?: 0)
        result = 31 * result + value.contentHashCode()
        return result
    }
}
