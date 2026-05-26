package com.example.mykafka.server

import com.example.mykafka.log.RecordCodec
import com.example.mykafka.protocol.ApiKey
import com.example.mykafka.protocol.Frame
import com.example.mykafka.topic.LogManager
import com.example.mykafka.topic.OffsetStore
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler
import org.slf4j.LoggerFactory
import java.nio.charset.StandardCharsets

// Frame → ApiKey 별 핸들러로 분기. Step 5에선 모든 요청이 topic 기반.
//
// 와이어 포맷
// ─────────────────────────────────────────────────────────────────────────
// CREATE_TOPIC
//   req : [topicLen:2][topic][partitionCount:4]
//   resp: [status:1]  (0=OK, 1=ALREADY_EXISTS, 2=INVALID)
//
// PRODUCE (Step 6 batch)
//   req : [topicLen:2][topic][partition:4][recordCount:4]
//         for each record: [keyLen:4][key][valLen:4][value]
//         - partition == -1   → broker가 partitioner로 결정 (첫 record의 key 사용)
//         - keyLen   == -1   → null key
//         - 같은 batch의 N개 record는 모두 같은 (topic, partition)에 들어간다.
//           이건 단순화가 아니라 실 Kafka도 동일 — client가 partition별로 grouping해서 보낸다.
//   resp: [errCode:1][partition:4][baseOffset:8][count:4]
//         - errCode 0 = OK, 1 = UNKNOWN_TOPIC, 2 = UNKNOWN_PARTITION, 3 = INVALID_BATCH
//         - 개별 record offset = baseOffset, baseOffset+1, …, baseOffset+count-1
//
// 왜 batch 의미를 정식화하나?
//   - 네트워크 RTT + 락 + syscall 비용을 한 번에 amortize → throughput의 핵심.
//   - "한 batch는 한 단위" 라는 의미를 broker가 보장 → 부분 성공 없음.
//     (record 일부만 쓰여 있는 상태는 Step 2 꼬리 자르기로 복구 단계에서 정리.)
//
// FETCH (Step 7 batch — pull-based consumer)
//   req : [topicLen:2][topic][partition:4][offset:8][maxBytes:4]
//   resp: [errCode:1][recordCount:4][record1 bytes][record2 bytes]…
//         - record는 RecordCodec 포맷 그대로 (self-delimiting). client는 N번 decode.
//         - errCode 0 = OK, 1 = UNKNOWN_TOPIC_OR_PARTITION
//         - offset이 nextOffset 이상이면 recordCount=0 (정상). consumer는 잠시 후 다시 시도.
//         - 적어도 1건은 보장 (record가 maxBytes보다 커도 보냄) — consumer 진행 보장.
//
// 왜 pull인가?
//   - broker는 그저 "이 offset부터 maxBytes만큼" 응답할 뿐, consumer 상태 추적 안 함.
//   - consumer가 자기 페이스대로 요청 → 백프레셔 자동 해결.
//   - broker가 거의 stateless → 컨슈머가 죽었다 살아나도 broker는 영향 없음.
//
// 왜 컨슈머가 offset을 들고 다니는가?
//   - broker가 컨슈머별 offset을 들고 있으면: consumer 수 ∝ broker 메모리/상태.
//   - 컨슈머가 들고 있으면: broker는 매 요청에 응답만, 컨슈머가 자기 진도 관리.
//   - 재처리도 자유 (과거 offset으로 복귀하면 같은 메시지 다시 받음).
//
// COMMIT_OFFSET (Step 8)
//   req : [groupLen:2][group][topicLen:2][topic][partition:4][offset:8]
//   resp: [errCode:1]   (0=OK)
//
// FETCH_OFFSET (Step 8)
//   req : [groupLen:2][group][topicLen:2][topic][partition:4]
//   resp: [errCode:1][offset:8]   (offset=-1 → 미커밋)
//
// 둘 다 내부 토픽 __consumer_offsets 에 저장되어 broker 재시작에도 살아남는다.
// "offset 저장도 그냥 또 다른 로그 append" — Kafka의 일관성 철학.
class RequestRouter(
    private val logManager: LogManager,
    private val offsetStore: OffsetStore,
) : SimpleChannelInboundHandler<Frame>() {
    private val slog = LoggerFactory.getLogger(javaClass)

    override fun channelRead0(ctx: ChannelHandlerContext, msg: Frame) {
        try {
            when (msg.apiKey) {
                ApiKey.PRODUCE -> handleProduce(ctx, msg.payload)
                ApiKey.FETCH -> handleFetch(ctx, msg.payload)
                ApiKey.CREATE_TOPIC -> handleCreateTopic(ctx, msg.payload)
                ApiKey.COMMIT_OFFSET -> handleCommitOffset(ctx, msg.payload)
                ApiKey.FETCH_OFFSET -> handleFetchOffset(ctx, msg.payload)
            }
        } finally {
            msg.payload.release()
        }
    }

    private fun handleCreateTopic(ctx: ChannelHandlerContext, payload: ByteBuf) {
        val topic = readString(payload)
        val partitionCount = payload.readInt()
        val result = logManager.createTopic(topic, partitionCount)
        val status: Byte = when (result) {
            LogManager.CreateResult.OK -> 0
            LogManager.CreateResult.ALREADY_EXISTS -> 1
            LogManager.CreateResult.INVALID -> 2
        }
        slog.info("CREATE_TOPIC topic={} partitions={} status={}", topic, partitionCount, status)
        ctx.writeAndFlush(Frame(ApiKey.CREATE_TOPIC, Unpooled.buffer(1).apply { writeByte(status.toInt()) }))
    }

    private fun handleProduce(ctx: ChannelHandlerContext, payload: ByteBuf) {
        val topic = readString(payload)
        val requestedPartition = payload.readInt()
        val recordCount = payload.readInt()

        if (recordCount <= 0) {
            ctx.writeAndFlush(produceResp(3, -1, -1, 0)); return
        }

        // record들을 모두 디코드
        val records = ArrayList<Pair<ByteArray?, ByteArray>>(recordCount)
        for (i in 0 until recordCount) {
            val keyLen = payload.readInt()
            val key: ByteArray? = if (keyLen == -1) {
                null
            } else {
                require(keyLen >= 0) { "invalid keyLen=$keyLen" }
                ByteArray(keyLen).also { payload.readBytes(it) }
            }
            val valueLen = payload.readInt()
            require(valueLen >= 0) { "invalid valueLen=$valueLen" }
            val value = ByteArray(valueLen).also { payload.readBytes(it) }
            records.add(key to value)
        }

        // partition 결정: -1이면 첫 record의 key로 partitioner 실행
        val partition = if (requestedPartition == -1) {
            logManager.pickPartition(topic, records[0].first)
        } else requestedPartition

        if (partition == null) {
            ctx.writeAndFlush(produceResp(1, -1, -1, 0)); return
        }
        val log = logManager.getPartition(topic, partition)
        if (log == null) {
            ctx.writeAndFlush(produceResp(2, partition, -1, 0)); return
        }

        val baseOffset = log.appendBatch(records)
        slog.info(
            "PRODUCE topic={} partition={} baseOffset={} count={}",
            topic, partition, baseOffset, recordCount,
        )
        ctx.writeAndFlush(produceResp(0, partition, baseOffset, recordCount))
    }

    private fun handleFetch(ctx: ChannelHandlerContext, payload: ByteBuf) {
        val topic = readString(payload)
        val partition = payload.readInt()
        val offset = payload.readLong()
        val maxBytes = payload.readInt()

        val log = logManager.getPartition(topic, partition)
        if (log == null) {
            ctx.writeAndFlush(fetchResp(errCode = 1, recordsBytes = ByteArray(0), count = 0))
            return
        }
        val recs = log.read(offset, maxBytes)
        slog.info(
            "FETCH topic={} partition={} offset={} maxBytes={} returned={}",
            topic, partition, offset, maxBytes, recs.size,
        )
        // record bytes 연결
        val perRecord = recs.map { RecordCodec.encode(it.offset, it.timestamp, it.key, it.value) }
        val totalLen = perRecord.sumOf { it.size }
        val merged = ByteArray(totalLen)
        var p = 0
        for (b in perRecord) {
            System.arraycopy(b, 0, merged, p, b.size); p += b.size
        }
        ctx.writeAndFlush(fetchResp(errCode = 0, recordsBytes = merged, count = recs.size))
    }

    private fun handleCommitOffset(ctx: ChannelHandlerContext, payload: ByteBuf) {
        val group = readString(payload)
        val topic = readString(payload)
        val partition = payload.readInt()
        val offset = payload.readLong()
        offsetStore.commit(group, topic, partition, offset)
        slog.info("COMMIT_OFFSET group={} topic={} partition={} offset={}", group, topic, partition, offset)
        ctx.writeAndFlush(Frame(ApiKey.COMMIT_OFFSET, Unpooled.buffer(1).apply { writeByte(0) }))
    }

    private fun handleFetchOffset(ctx: ChannelHandlerContext, payload: ByteBuf) {
        val group = readString(payload)
        val topic = readString(payload)
        val partition = payload.readInt()
        val offset = offsetStore.fetch(group, topic, partition)
        slog.info("FETCH_OFFSET group={} topic={} partition={} → {}", group, topic, partition, offset)
        val resp = Unpooled.buffer(1 + 8).apply {
            writeByte(0)
            writeLong(offset)
        }
        ctx.writeAndFlush(Frame(ApiKey.FETCH_OFFSET, resp))
    }

    private fun produceResp(errCode: Int, partition: Int, baseOffset: Long, count: Int): Frame {
        val buf = Unpooled.buffer(1 + 4 + 8 + 4)
        buf.writeByte(errCode)
        buf.writeInt(partition)
        buf.writeLong(baseOffset)
        buf.writeInt(count)
        return Frame(ApiKey.PRODUCE, buf)
    }

    private fun fetchResp(errCode: Int, recordsBytes: ByteArray, count: Int): Frame {
        val buf = Unpooled.buffer(1 + 4 + recordsBytes.size)
        buf.writeByte(errCode)
        buf.writeInt(count)
        if (recordsBytes.isNotEmpty()) buf.writeBytes(recordsBytes)
        return Frame(ApiKey.FETCH, buf)
    }

    private fun readString(payload: ByteBuf): String {
        val len = payload.readShort().toInt()
        require(len in 0..1024) { "invalid string len=$len" }
        val arr = ByteArray(len).also { payload.readBytes(it) }
        return String(arr, StandardCharsets.UTF_8)
    }

    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
        slog.error("channel error", cause)
        ctx.close()
    }
}
