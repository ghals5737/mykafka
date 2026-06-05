package com.example.mykafka.broker

import com.example.mykafka.protocol.ApiKey

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.ByteToMessageDecoder
import io.netty.handler.codec.MessageToByteEncoder

// Netty 디코더: 들어오는 바이트 스트림에서 한 프레임을 잘라낸다.
//
// 동작:
//   1. 4바이트(length) 다 안 오면 대기
//   2. length를 읽어 그 길이만큼 더 안 오면 대기 (롤백)
//   3. 충분하면 apiKey 1바이트 + payload(length-1)를 한 Frame으로 emit
//
// 왜 ByteToMessageDecoder를 쓰나?
//   Netty가 "데이터 부족 시 자동 대기/누적" 처리를 해주기 때문.
//   직접 짜면 fragmentation/partial read 버그가 잘 난다.
class FrameDecoder : ByteToMessageDecoder() {
    override fun decode(ctx: ChannelHandlerContext, input: ByteBuf, out: MutableList<Any>) {
        if (input.readableBytes() < 4) return // length 헤더 부족

        input.markReaderIndex()
        val totalLength = input.readInt()
        if (totalLength < 1) {
            // 최소한 apiKey(1바이트)는 있어야 함
            throw IllegalStateException("Invalid frame length: $totalLength")
        }

        if (input.readableBytes() < totalLength) {
            // payload 아직 다 안 옴 → 롤백하고 다음 read 기다림
            input.resetReaderIndex()
            return
        }

        val apiKeyCode = input.readByte()
        val payloadLength = totalLength - 1
        // payload는 슬라이스 + retain — 핸들러가 다 쓰고 release 해야 함
        val payload = input.readRetainedSlice(payloadLength)

        out.add(Frame(ApiKey.from(apiKeyCode), payload))
    }
}

// Netty 인코더: Frame을 바이트 스트림으로 직렬화.
class FrameEncoder : MessageToByteEncoder<Frame>() {
    override fun encode(ctx: ChannelHandlerContext, msg: Frame, out: ByteBuf) {
        val payloadLength = msg.payload.readableBytes()
        out.writeInt(1 + payloadLength) // totalLength = apiKey(1) + payload
        out.writeByte(msg.apiKey.code.toInt())
        out.writeBytes(msg.payload)
        // payload는 호출 측이 retain 한 ByteBuf이므로 release 책임 분리
        msg.payload.release()
    }
}
