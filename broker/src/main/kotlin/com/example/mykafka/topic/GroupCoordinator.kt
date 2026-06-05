package com.example.mykafka.topic

import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicInteger

// Consumer group의 멤버십을 추적하고 파티션을 자동 할당한다 (학습용 MVP).
//
// 단순화 (실 Kafka 대비):
//   - 할당을 broker가 직접 계산해서 각 멤버에 통보 (Kafka는 client-leader가 계산).
//   - JoinGroup이 heartbeat 겸용 = 주기적 재호출로 lease 갱신 (별도 Heartbeat API 없음).
//   - 멤버 만료는 다음 join 때 lazy 처리 (백그라운드 타이머 없음).
//   - 할당 전략은 round-robin (p % memberCount).
//   - 멤버십은 in-memory (broker 재시작 시 전원 재join). offset은 OffsetStore에 durable.
//
// 동기화: 여러 Netty worker 스레드에서 호출 → 메서드 전체 @Synchronized (그룹 수가 적어 충분).
class GroupCoordinator(private val nowMs: () -> Long = System::currentTimeMillis) {
    private val log = LoggerFactory.getLogger(javaClass)

    data class JoinResult(val memberId: String, val generation: Int, val assigned: List<Int>)

    private class GroupState {
        val members = LinkedHashMap<String, Long>() // memberId -> lastSeenMs
        var generation = 0
    }

    private val groups = HashMap<String, GroupState>()
    private val seq = AtomicInteger(0)

    @Synchronized
    fun join(group: String, memberId: String, sessionTimeoutMs: Long, partitionCount: Int): JoinResult {
        val g = groups.getOrPut(group) { GroupState() }
        val now = nowMs()
        val before = g.members.keys.toSet()

        // 1) 만료된 멤버 제거 (지금 갱신 중인 멤버는 보호)
        g.members.entries.removeIf { (id, lastSeen) ->
            id != memberId && now - lastSeen > sessionTimeoutMs
        }

        // 2) 멤버 등록/갱신 (id 없으면 발급)
        val id = if (memberId.isEmpty()) "m-${seq.incrementAndGet()}" else memberId
        g.members[id] = now

        // 3) 멤버 집합이 바뀌었으면 generation++ (= rebalance 발생)
        if (g.members.keys.toSet() != before) {
            g.generation++
            log.info("group={} rebalance gen={} members={}", group, g.generation, g.members.keys)
        }

        // 4) round-robin 할당 — memberId 정렬 후 p % memberCount == myIndex
        val sorted = g.members.keys.sorted()
        val myIndex = sorted.indexOf(id)
        val n = sorted.size
        val assigned = (0 until partitionCount).filter { it % n == myIndex }

        return JoinResult(id, g.generation, assigned)
    }
}
