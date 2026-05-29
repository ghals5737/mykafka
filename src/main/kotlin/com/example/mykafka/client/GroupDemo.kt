package com.example.mykafka.client

// Consumer group 자동 파티션 할당 데모.
// 같은 group의 멤버 여러 명이 joinGroup → broker가 파티션을 겹치지 않게 분배.
// 멤버가 만료(lease timeout)되면 재분배(rebalance) + generation 증가.
//
// 전제: broker가 :9092에 떠 있어야 함.
fun main() {
    val topic = "group-demo-topic"
    val partitions = 3
    val sessionTimeoutMs = 1000 // 데모용 짧은 lease (만료를 빨리 보기 위함)

    MyKafkaProducer().use { it.createTopic(topic, partitions) }
    println("topic=$topic partitions=$partitions, sessionTimeout=${sessionTimeoutMs}ms\n")

    val c1 = MyKafkaConsumer(group = "gd"); val c2 = MyKafkaConsumer(group = "gd"); val c3 = MyKafkaConsumer(group = "gd")

    println("── 1) 멤버 3명 순차 가입 ──")
    c1.joinGroup(topic, sessionTimeoutMs)
    c2.joinGroup(topic, sessionTimeoutMs)
    c3.joinGroup(topic, sessionTimeoutMs)
    // 전원 가입 후 한 바퀴 더 join해야 각자 최종 분배를 본다 (broker는 join 시점 멤버로 계산).
    val a1 = c1.joinGroup(topic, sessionTimeoutMs)
    val a2 = c2.joinGroup(topic, sessionTimeoutMs)
    val a3 = c3.joinGroup(topic, sessionTimeoutMs)
    println("   ${a1.memberId} gen=${a1.generation} → partitions ${a1.partitions}")
    println("   ${a2.memberId} gen=${a2.generation} → partitions ${a2.partitions}")
    println("   ${a3.memberId} gen=${a3.generation} → partitions ${a3.partitions}")
    val union = (a1.partitions + a2.partitions + a3.partitions).sorted()
    println("   합집합=$union  (겹침 없음/전부 커버: ${union == (0 until partitions).toList()})\n")

    println("── 2) c3 이탈 (재join 안 함) → lease 만료 대기 ──")
    Thread.sleep((sessionTimeoutMs + 500).toLong())
    // 만료 트리거 + 재등록 라운드, 그 뒤 정착(settle) 라운드로 둘 다 최종 분배를 보게 한다.
    c1.joinGroup(topic, sessionTimeoutMs); c2.joinGroup(topic, sessionTimeoutMs)
    val b1 = c1.joinGroup(topic, sessionTimeoutMs)
    val b2 = c2.joinGroup(topic, sessionTimeoutMs)
    println("   ${b1.memberId} gen=${b1.generation} → partitions ${b1.partitions}")
    println("   ${b2.memberId} gen=${b2.generation} → partitions ${b2.partitions}")
    val union2 = (b1.partitions + b2.partitions).sorted()
    println("   합집합=$union2  (2명이 3파티션 분담, generation 증가: ${b1.generation > a1.generation})\n")

    println("── 3) 다른 group은 독립 ──")
    val other = MyKafkaConsumer(group = "other-gd").use { it.joinGroup(topic, sessionTimeoutMs) }
    println("   other-gd의 멤버 → partitions ${other.partitions} (혼자라 전 파티션)")

    c1.close(); c2.close(); c3.close()
}
