// MyKafka 브로커 — Netty 기반 단일 노드 서버. protocol에 의존.
//   실행: ./gradlew :broker:run --args="9092 data"
plugins {
    kotlin("jvm")
    application
    `maven-publish`
}

dependencies {
    implementation(project(":protocol"))
    implementation("io.netty:netty-all:4.1.115.Final")
    implementation("org.slf4j:slf4j-simple:2.0.13")
    implementation("org.jetbrains.kotlin:kotlin-stdlib")
}

application {
    mainClass.set("com.example.mykafka.MainKt")
}

kotlin {
    jvmToolchain(17)
    compilerOptions { jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_17) }
}

java { withSourcesJar() }

publishing {
    publications { create<MavenPublication>("maven") { from(components["java"]) } }
}
