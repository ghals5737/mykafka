// MyKafka — Kafka의 코어 로직을 직접 구현한 학습용 카프카 클론.
// 3모듈: protocol(와이어 포맷) / client(Producer·Consumer SDK) / broker(서버).
plugins {
    kotlin("jvm") version "2.2.21" apply false
}

allprojects {
    group = "com.example.mykafka"
    version = "0.1.0"
    repositories { mavenCentral() }
}
