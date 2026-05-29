package com.example.mykafka.protocol

// 자체 wire protocol의 API 식별자.
// 실제 Kafka는 수십 개의 ApiKey가 있지만, MVP는 3개로 시작한다.
enum class ApiKey(val code: Byte) {
    PRODUCE(0),
    FETCH(1),
    CREATE_TOPIC(2),
    COMMIT_OFFSET(3),
    FETCH_OFFSET(4),
    JOIN_GROUP(5);

    companion object {
        fun from(code: Byte): ApiKey =
            entries.firstOrNull { it.code == code }
                ?: error("Unknown ApiKey: $code")
    }
}
