package com.example.mykafka.client

import com.example.mykafka.protocol.ApiKey
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.net.Socket
import java.nio.charset.StandardCharsets

// MyKafka 동기 Producer 클라이언트.
//
// 설계 선택
//   - raw Socket + DataInput/OutputStream — wire format을 한 줄씩 그대로 다룬다.
//     "Netty client + correlation id + async pipelining" 같은 production 기능은 의도적으로 생략.
//     wire를 직접 보는 것이 학습 가치.
//   - 동기(blocking) — 요청 1건마다 응답 받기까지 block. 단순.
//   - 단일 connection — 인스턴스 하나당 한 TCP 연결. thread-safe X.
//     여러 thread에서 쓰려면 외부 동기화 또는 connection pool.
//
// 응답 매칭
//   correlation id가 없으니 "보낸 순서대로 응답이 옴" 에 의존. 한 connection 안에서 직렬이라 OK.
//   pipelining(N 요청 → N 응답)이 필요하면 응답을 큐로 모아 매칭하는 layer 추가 필요.
class MyKafkaProducer(
    host: String = "localhost",
    port: Int = 9092,
) : AutoCloseable {
    private val socket: Socket = Socket(host, port)
    private val out = DataOutputStream(socket.getOutputStream().buffered())
    private val input = DataInputStream(socket.getInputStream().buffered())

    init {
        // 학습 측정용: Nagle off → 작은 요청도 즉시 전송 (latency 측정 의미 있게)
        socket.tcpNoDelay = true
    }

    data class ProduceResult(
        val partition: Int,
        val baseOffset: Long,
        val count: Int,
    ) {
        val offsets: LongRange get() = baseOffset until (baseOffset + count)
    }

    enum class CreateTopicResult { OK, ALREADY_EXISTS, INVALID }

    // ── CREATE_TOPIC ─────────────────────────────────────────────────────
    // req : [topicLen:2][topic][partitionCount:4]
    // resp: [status:1]    0=OK, 1=ALREADY_EXISTS, 2=INVALID
    fun createTopic(topic: String, partitionCount: Int): CreateTopicResult {
        val topicBytes = topic.toByteArray(StandardCharsets.UTF_8)
        sendFrame(ApiKey.CREATE_TOPIC) { buf ->
            buf.writeShort(topicBytes.size)
            buf.write(topicBytes)
            buf.writeInt(partitionCount)
        }
        val (apiKey, _) = readFrameHeader()
        check(apiKey == ApiKey.CREATE_TOPIC) { "expected CREATE_TOPIC response, got $apiKey" }
        return when (val status = input.readByte().toInt()) {
            0 -> CreateTopicResult.OK
            1 -> CreateTopicResult.ALREADY_EXISTS
            2 -> CreateTopicResult.INVALID
            else -> error("unknown CREATE_TOPIC status=$status")
        }
    }

    // ── PRODUCE (단건 helper) ────────────────────────────────────────────
    fun produce(topic: String, key: ByteArray?, value: ByteArray, partition: Int = -1): ProduceResult =
        produceBatch(topic, listOf(key to value), partition)

    fun produce(topic: String, key: String?, value: String, partition: Int = -1): ProduceResult =
        produce(topic, key?.toByteArray(), value.toByteArray(), partition)

    // ── PRODUCE batch ────────────────────────────────────────────────────
    // req : [topicLen:2][topic][partition:4][recordCount:4]
    //       for each record: [keyLen:4][key][valLen:4][value]
    //
    //       partition == -1  → broker가 partitioner로 결정 (첫 record의 key 사용)
    //       keyLen    == -1  → null key
    //
    // resp: [errCode:1][partition:4][baseOffset:8][count:4]
    //       errCode: 0=OK, 1=UNKNOWN_TOPIC, 2=UNKNOWN_PARTITION, 3=INVALID_BATCH
    //
    // 학습 메모: batch 가 throughput 의 비밀이다.
    //   - 같은 데이터 양도 batch 1번 vs 단건 N번 → 네트워크 RTT/락/syscall 비용이 N분의 1.
    //   - MyKafka MVP 측정: 1000건 단건 84ms vs 1×1000 batch 1.4ms = 61x.
    fun produceBatch(
        topic: String,
        records: List<Pair<ByteArray?, ByteArray>>,
        partition: Int = -1,
    ): ProduceResult {
        require(records.isNotEmpty()) { "empty batch" }
        val topicBytes = topic.toByteArray(StandardCharsets.UTF_8)
        sendFrame(ApiKey.PRODUCE) { buf ->
            buf.writeShort(topicBytes.size)
            buf.write(topicBytes)
            buf.writeInt(partition)
            buf.writeInt(records.size)
            for ((k, v) in records) {
                if (k == null) {
                    buf.writeInt(-1)
                } else {
                    buf.writeInt(k.size)
                    buf.write(k)
                }
                buf.writeInt(v.size)
                buf.write(v)
            }
        }
        val (apiKey, _) = readFrameHeader()
        check(apiKey == ApiKey.PRODUCE) { "expected PRODUCE response, got $apiKey" }
        val errCode = input.readByte().toInt()
        val respPartition = input.readInt()
        val baseOffset = input.readLong()
        val count = input.readInt()
        if (errCode != 0) {
            error("PRODUCE errCode=$errCode (1=UNKNOWN_TOPIC, 2=UNKNOWN_PARTITION, 3=INVALID_BATCH)")
        }
        return ProduceResult(respPartition, baseOffset, count)
    }

    // ── 내부 ─────────────────────────────────────────────────────────────

    // 단일 frame 전송:
    //   1) 임시 buffer 에 payload write
    //   2) totalLength = 1(apiKey) + payload.size 먼저 전송
    //   3) apiKey 1바이트
    //   4) payload
    //
    // trade-off: payload 사이즈를 미리 알아야 length-prefix 가능 → 메모리에 모았다가 전송.
    //   매우 큰 batch면 memory pressure. production client 는 chunked write 같은 고민이 추가됨.
    private inline fun sendFrame(apiKey: ApiKey, build: (DataOutputStream) -> Unit) {
        val bytes = ByteArrayOutputStream()
        val tmp = DataOutputStream(bytes)
        build(tmp)
        tmp.flush()
        val payload = bytes.toByteArray()
        out.writeInt(1 + payload.size)         // totalLength = apiKey(1) + payload
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
