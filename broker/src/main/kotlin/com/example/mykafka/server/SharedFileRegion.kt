package com.example.mykafka.server

import io.netty.channel.DefaultFileRegion
import java.nio.channels.FileChannel

// Segment의 "이미 열려 있는" FileChannel을 재사용하는 FileRegion.
//
// 왜 필요한가:
//   기본 DefaultFileRegion(File,...)은 전송할 때마다 파일을 open() 하고,
//   DefaultFileRegion(FileChannel,...)은 전송 후 deallocate()에서 그 채널을 close() 해버린다.
//   둘 다 우리 케이스에 안 맞다 — 세그먼트 채널은 append/read에 계속 쓰이는 공유 자원이라
//   fetch마다 열거나 닫으면 안 된다.
//
//   그래서 채널을 그대로 받아 transferTo만 하고, deallocate에서는 채널을 닫지 않는다.
//   (채널 수명은 Segment.close()가 관리.)
//
// 안전성: FileChannel.transferTo(position, …)는 채널 position을 바꾸지 않는 positional 연산이라
//   append(position 기반 write, Log.lock 하에)와 동시에 일어나도 안전하다. 전송 구간은 이미
//   커밋된 바이트라 진행 중인 append와 겹치지 않는다.
class SharedFileRegion(
    channel: FileChannel,
    position: Long,
    count: Long,
) : DefaultFileRegion(channel, position, count) {
    override fun deallocate() {
        // 공유 채널이므로 닫지 않는다 (의도적 no-op).
    }
}
