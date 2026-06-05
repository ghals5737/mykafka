// 와이어 포맷만 (ApiKey, Record, RecordCodec). netty 없음 → client가 가볍게 의존.
plugins {
    kotlin("jvm")
    `maven-publish`
}

dependencies {
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
