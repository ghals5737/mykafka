package com.example.mykafka.client

import com.example.mykafka.log.Record
import com.example.mykafka.log.RecordCodec
import com.example.mykafka.protocol.ApiKey
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.net.Socket
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

// MyKafka 동기 Consumer 클라이언트.
//
// 설계 선택 (Producer 와 동일)
//   - raw Socket. wire format을 한 줄씩 그대로 다룬다.
//   - 동기(blocking). poll → 결과 받으면 처리 → commit → 다음 poll.
//   - 단일 connection. thread-safe X.
//
// Kafka 정체성 핵심
//   - Pull-based: broker는 우리 상태를 모름. 우리가 offset 지정해서 fetch.
//   - Consumer-managed offset: 우리가 직접 commit 해야 broker가 기억함.
//     안 하면 재시작 시 처음부터(또는 어디부터 시작할지 application 결정).
//   - Replay 자유: 어떤 offset이든 다시 fetch 가능. (실 Kafka도 동일)
class MyKafkaConsumer(
    host: String = "localhost",
    port: Int = 9092,
    val group: String,
) : AutoCloseable {
    private val socket: Socket = Socket(host, port)
    private val out = DataOutputStream(socket.getOutputStream().buffered())
    private val input = DataInputStream(socket.getInputStream().buffered())

    init { socket.tcpNoDelay = true }

    data class FetchResult(
        val records: List<Record>,
        /** 다음 fetch 에 넣을 offset (records 마지막 + 1, 비었으면 요청한 offset 그대로) */
        val nextOffset: Long,
    )

    // ── FETCH ────────────────────────────────────────────────────────────
    // req : [topicLen:2][topic][partition:4][offset:8][maxBytes:4]
    // resp: [errCode:1][recordCount:4][record1 bytes][record2 bytes]…
    //
    // 진행 보장: record 1건이 maxBytes 보다 커도 1건은 무조건 포함된다.
    //   안 그러면 컨슈머가 영원히 못 받아 stuck. Kafka 동일 invariant.
    fun fetch(topic: String, partition: Int, offset: Long, maxBytes: Int = 4096): FetchResult {
        val topicBytes = topic.toByteArray(StandardCharsets.UTF_8)
        sendFrame(ApiKey.FETCH) { buf ->
            buf.writeShort(topicBytes.size)
            buf.write(topicBytes)
            buf.writeInt(partition)
            buf.writeLong(offset)
            buf.writeInt(maxBytes)
        }
        val (apiKey, payloadLen) = readFrameHeader()
        check(apiKey == ApiKey.FETCH) { "expected FETCH response, got $apiKey" }
        val errCode = input.readByte().toInt()
        if (errCode != 0) error("FETCH errCode=$errCode (1=UNKNOWN_TOPIC_OR_PARTITION)")
        val recordCount = input.readInt()

        // record bytes 길이 = payload - errCode(1) - recordCount(4)
        val recordBytesLen = payloadLen - 5
        val recordsBytes = ByteArray(recordBytesLen).also { input.readFully(it) }

        // self-delimiting record들을 N번 decode
        val buf = ByteBuffer.wrap(recordsBytes)
        val records = ArrayList<Record>(recordCount)
        repeat(recordCount) {
            val rec = RecordCodec.decode(buf)
                ?: error("FETCH response truncated at record ${records.size}/$recordCount")
            records.add(rec)
        }
        val nextOffset = if (records.isEmpty()) offset else records.last().offset + 1
        return FetchResult(records, nextOffset)
    }

    // ── COMMIT_OFFSET ────────────────────────────────────────────────────
    // req : [groupLen:2][group][topicLen:2][topic][partition:4][offset:8]
    // resp: [errCode:1]
    //
    // 의미: "이 (group, topic, partition) 에서 offset 까지 (= offset 직전까지) 처리 완료."
    //   다음 재시작/poll 에서 이 commit 된 offset 부터 시작하면 됨.
    fun commitOffset(topic: String, partition: Int, offset: Long) {
        val groupBytes = group.toByteArray(StandardCharsets.UTF_8)
        val topicBytes = topic.toByteArray(StandardCharsets.UTF_8)
        sendFrame(ApiKey.COMMIT_OFFSET) { buf ->
            buf.writeShort(groupBytes.size); buf.write(groupBytes)
            buf.writeShort(topicBytes.size); buf.write(topicBytes)
            buf.writeInt(partition)
            buf.writeLong(offset)
        }
        val (apiKey, _) = readFrameHeader()
        check(apiKey == ApiKey.COMMIT_OFFSET) { "expected COMMIT_OFFSET response, got $apiKey" }
        val errCode = input.readByte().toInt()
        if (errCode != 0) error("COMMIT_OFFSET errCode=$errCode")
    }

    // ── FETCH_OFFSET ─────────────────────────────────────────────────────
    // req : [groupLen:2][group][topicLen:2][topic][partition:4]
    // resp: [errCode:1][offset:8]    offset=-1 → 이 group 이 이 (topic, partition) 을 commit 한 적 없음
    //
    // 의미: "지난번에 어디까지 처리했는지 broker 한테 물어보기."
    //   재시작 시 첫 호출. 결과가 -1 이면 application 이 "처음부터?" 또는 "지금부터?" 결정.
    fun fetchOffset(topic: String, partition: Int): Long {
        val groupBytes = group.toByteArray(StandardCharsets.UTF_8)
        val topicBytes = topic.toByteArray(StandardCharsets.UTF_8)
        sendFrame(ApiKey.FETCH_OFFSET) { buf ->
            buf.writeShort(groupBytes.size); buf.write(groupBytes)
            buf.writeShort(topicBytes.size); buf.write(topicBytes)
            buf.writeInt(partition)
        }
        val (apiKey, _) = readFrameHeader()
        check(apiKey == ApiKey.FETCH_OFFSET) { "expected FETCH_OFFSET response, got $apiKey" }
        val errCode = input.readByte().toInt()
        if (errCode != 0) error("FETCH_OFFSET errCode=$errCode")
        return input.readLong()
    }

    // ── 편의: poll (자동 offset 관리) ────────────────────────────────────
    // 마지막 commit (없으면 0) 부터 fetch.
    // 처리 후 commitOffset(...nextOffset) 명시적 호출 책임은 caller.
    //
    // 의도적 — commit을 자동화 안 함:
    //   "auto-commit이 데이터 손실의 가장 흔한 원인"이라는 Kafka 운영 격언 그대로.
    //   처리 (DB INSERT 등) 가 성공한 뒤에 commit 해야 at-least-once 가 보장됨.
    fun poll(topic: String, partition: Int, maxBytes: Int = 4096): FetchResult {
        val committed = fetchOffset(topic, partition)
        val startOffset = if (committed < 0) 0L else committed
        return fetch(topic, partition, startOffset, maxBytes)
    }

    // ── 내부 (Producer 와 동일 패턴; 학습용으로 복사. 실 운영이면 BaseClient 추출.) ──
    private inline fun sendFrame(apiKey: ApiKey, build: (DataOutputStream) -> Unit) {
        val bytes = ByteArrayOutputStream()
        val tmp = DataOutputStream(bytes)
        build(tmp)
        tmp.flush()
        val payload = bytes.toByteArray()
        out.writeInt(1 + payload.size)
        out.writeByte(apiKey.code.toInt())
        out.write(payload)
        out.flush()
    }

    private fun readFrameHeader(): Pair<ApiKey, Int> {
        val totalLength = input.readInt()
        val apiKeyByte = input.readByte()
        return ApiKey.from(apiKeyByte) to (totalLength - 1)
    }

    override fun close() {
        runCatching { input.close() }
        runCatching { out.close() }
        runCatching { socket.close() }
    }
}
