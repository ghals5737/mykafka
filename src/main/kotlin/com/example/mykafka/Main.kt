package com.example.mykafka

import com.example.mykafka.log.Log
import com.example.mykafka.server.BrokerServer
import java.nio.file.Path

// 인자:
//   args[0] = port               (기본 9092)
//   args[1] = dataDir            (기본 "data")
//   args[2] = maxSegmentBytes    (기본 Log.DEFAULT_MAX_SEGMENT_BYTES)
//   args[3] = indexIntervalBytes (기본 Log.DEFAULT_INDEX_INTERVAL_BYTES)
fun main(args: Array<String>) {
    val port = args.firstOrNull()?.toIntOrNull() ?: 9092
    val dataDir = Path.of(args.getOrNull(1) ?: "data")
    val maxSegmentBytes = args.getOrNull(2)?.toLongOrNull() ?: Log.DEFAULT_MAX_SEGMENT_BYTES
    val indexIntervalBytes = args.getOrNull(3)?.toIntOrNull() ?: Log.DEFAULT_INDEX_INTERVAL_BYTES
    BrokerServer(port, dataDir, maxSegmentBytes, indexIntervalBytes).start()
}
