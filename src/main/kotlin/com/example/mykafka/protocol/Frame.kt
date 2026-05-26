package com.example.mykafka.protocol

import io.netty.buffer.ByteBuf

// 한 메시지 단위 = "프레임".
// wire format:  [4 bytes totalLength][1 byte apiKey][payload...]
//   - totalLength: apiKey(1) + payload 길이 (4바이트 length 필드 자신은 제외)
//
// 왜 length-prefix framing 인가?
//   TCP는 바이트 스트림이라 메시지 경계가 없다.
//   "어디까지가 한 메시지인지"를 수신자가 알기 위해 길이를 먼저 보낸다.
//   실제 Kafka 프로토콜도 동일한 구조 (Size + Header + Body).
data class Frame(
    val apiKey: ApiKey,
    val payload: ByteBuf,
)
