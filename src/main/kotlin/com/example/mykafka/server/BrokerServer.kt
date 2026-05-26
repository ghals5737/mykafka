package com.example.mykafka.server

import com.example.mykafka.log.Log
import com.example.mykafka.protocol.FrameDecoder
import com.example.mykafka.protocol.FrameEncoder
import com.example.mykafka.topic.LogManager
import com.example.mykafka.topic.OffsetStore
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.ChannelInitializer
import io.netty.channel.ChannelOption
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import org.slf4j.LoggerFactory
import java.nio.file.Path

class BrokerServer(
    private val port: Int,
    private val dataDir: Path,
    private val maxSegmentBytes: Long = Log.DEFAULT_MAX_SEGMENT_BYTES,
    private val indexIntervalBytes: Int = Log.DEFAULT_INDEX_INTERVAL_BYTES,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    fun start() {
        val bossGroup = NioEventLoopGroup(1)
        val workerGroup = NioEventLoopGroup()
        val logManager = LogManager(dataDir, maxSegmentBytes, indexIntervalBytes)
        val offsetStore = OffsetStore(logManager)
        try {
            val bootstrap = ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel::class.java)
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(object : ChannelInitializer<SocketChannel>() {
                    override fun initChannel(ch: SocketChannel) {
                        ch.pipeline()
                            .addLast("decoder", FrameDecoder())
                            .addLast("encoder", FrameEncoder())
                            .addLast("router", RequestRouter(logManager, offsetStore))
                    }
                })

            val channel = bootstrap.bind(port).sync().channel()
            log.info(
                "MyKafka broker started on port {} dataDir={} maxSegmentBytes={} indexIntervalBytes={}",
                port, dataDir, maxSegmentBytes, indexIntervalBytes,
            )
            channel.closeFuture().sync()
        } finally {
            workerGroup.shutdownGracefully()
            bossGroup.shutdownGracefully()
            logManager.close()
        }
    }
}
