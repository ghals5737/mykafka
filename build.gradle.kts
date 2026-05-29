plugins {
    kotlin("jvm") version "2.2.21"
    application
}

group = "com.example"
version = "0.0.1-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("io.netty:netty-all:4.1.115.Final")
    implementation("org.jetbrains.kotlin:kotlin-stdlib")
    implementation("org.slf4j:slf4j-simple:2.0.13")
}

application {
    mainClass.set("com.example.mykafka.MainKt")
}

// 학습용 client demo 실행 task
//   ./gradlew producerDemo     (broker가 9092 에 떠 있어야 함)
tasks.register<JavaExec>("producerDemo") {
    group = "demo"
    description = "Run the Producer client demo (requires broker on :9092)"
    dependsOn("classes")
    mainClass.set("com.example.mykafka.client.ProducerDemoKt")
    classpath = sourceSets.main.get().runtimeClasspath
    standardInput = System.`in`
}

tasks.register<JavaExec>("consumerDemo") {
    group = "demo"
    description = "Run the Consumer client demo (requires broker on :9092 + topic seeded)"
    dependsOn("classes")
    mainClass.set("com.example.mykafka.client.ConsumerDemoKt")
    classpath = sourceSets.main.get().runtimeClasspath
    standardInput = System.`in`
}

// FETCH 처리량 벤치 (zero-copy vs heap). broker를 MYKAFKA_FETCH_ZEROCOPY on/off로 각각 띄우고 실행.
tasks.register<JavaExec>("fetchBench") {
    group = "demo"
    description = "FETCH throughput benchmark (requires broker on :9092)"
    dependsOn("classes")
    mainClass.set("com.example.mykafka.client.FetchBenchDemoKt")
    classpath = sourceSets.main.get().runtimeClasspath
    System.getenv("VALUE_SIZE")?.let { environment("VALUE_SIZE", it) }
    System.getenv("RECORD_COUNT")?.let { environment("RECORD_COUNT", it) }
    System.getenv("MAX_BYTES")?.let { environment("MAX_BYTES", it) }
    System.getenv("BENCH_MS")?.let { environment("BENCH_MS", it) }
}

kotlin {
    jvmToolchain(17)
    compilerOptions {
        jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_17)
    }
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}

tasks.withType<JavaCompile>().configureEach {
    sourceCompatibility = "17"
    targetCompatibility = "17"
}
