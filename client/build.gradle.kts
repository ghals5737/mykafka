// MyKafka 클라이언트 SDK — Producer / Consumer. raw Socket 기반이라 netty 의존 없음.
// 공개 API가 protocol의 타입(Record, ApiKey 등)을 노출하므로 api()로 전이 공개.
plugins {
    kotlin("jvm")
    `java-library`
    `maven-publish`
}

dependencies {
    api(project(":protocol"))
    implementation("org.jetbrains.kotlin:kotlin-stdlib")
}

kotlin {
    jvmToolchain(17)
    compilerOptions { jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_17) }
}

java { withSourcesJar() }

publishing {
    publications { create<MavenPublication>("maven") { from(components["java"]) } }
}

// ── 학습용 데모 실행 task (broker가 :9092에 떠 있어야 함) ──
tasks.register<JavaExec>("producerDemo") {
    group = "demo"; description = "Producer 데모"
    mainClass.set("com.example.mykafka.client.ProducerDemoKt")
    classpath = sourceSets.main.get().runtimeClasspath
    standardInput = System.`in`
}
tasks.register<JavaExec>("consumerDemo") {
    group = "demo"; description = "Consumer 데모"
    mainClass.set("com.example.mykafka.client.ConsumerDemoKt")
    classpath = sourceSets.main.get().runtimeClasspath
    standardInput = System.`in`
}
tasks.register<JavaExec>("groupDemo") {
    group = "demo"; description = "Consumer group 파티션 할당 데모"
    mainClass.set("com.example.mykafka.client.GroupDemoKt")
    classpath = sourceSets.main.get().runtimeClasspath
}
tasks.register<JavaExec>("fetchBench") {
    group = "demo"; description = "FETCH 처리량 벤치 (zero-copy vs heap)"
    mainClass.set("com.example.mykafka.client.FetchBenchDemoKt")
    classpath = sourceSets.main.get().runtimeClasspath
    listOf("VALUE_SIZE", "RECORD_COUNT", "MAX_BYTES", "BENCH_MS").forEach { k ->
        System.getenv(k)?.let { environment(k, it) }
    }
}
