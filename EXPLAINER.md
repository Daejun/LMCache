# L2 Eviction 워터마크 이벤트 트리거 — 구현 설명서

> 이번에 구현한 변경을 처음 보는 사람이 읽고 이해하도록 정리한 단독 문서.
> 브랜치 `l2-eviction-watermark-trigger`, 커밋 `b0e0708f [MP] l2 eviction: event-driven watermark trigger`.
> 대상: **MP(multiprocess) 모드의 L2 어댑터 계층**. (non-MP/in-process 경로와는 무관 — 뒤 §6 참고)

---

## 한 줄 요약

L2 캐시 eviction이 **1초 고정 폴링**으로 돌던 것을, **사용량이 워터마크를 넘는 순간 즉시 깨우는
이벤트 방식**으로 바꿨다. 포화 상황에서 "새 데이터 거부 / 헌 데이터 보존"으로 퇴화하던 캐시가
제때 회수·수용하도록 만든다.

---

## 1. 문제: 무엇이 잘못돼 있었나

L2 캐시의 회수(eviction)는 `L2EvictionController`라는 **백그라운드 스레드 하나**가 담당한다.
이 스레드의 루프는 이렇게 생겼었다:

```python
while not self._stop_flag.is_set():
    time.sleep(1)                      # ← 무조건 1초 잔다
    for state in self._adapter_states:
        self._check_and_evict(state)   # 사용량이 워터마크 넘으면 회수
```

즉 **"사용량이 꽉 찼는지"를 1초에 한 번만 확인**한다. 그래서 쓰기가 몰리면:

1. L2 슬롯이 수 ms 만에 가득 참.
2. 그런데 다음 확인은 최대 1초 뒤 → 그 1초 동안 **새 store가 전부 거부**됨.
   (raw_block 백엔드는 빈 슬롯이 없으면 `put_many`가 그냥 `False`를 반환한다.)
3. 1초 뒤에야 회수가 한 번 돌아 자리를 비우고, 다시 채워지고… 반복.

### 더 본질적인 문제 — "anti-LRU" 퇴화

용량과 정책(LRU)은 그대로인데 **타이밍만 늦는 게** 왜 나쁜가? 포화 구간에서 폴링 캐시는
*방금 계산된 최신 청크를 버리고(거부), 오래된 청크를 그대로 끌어안는다.* 이는 LRU의 의도와
정반대다. eviction이 제때 돌면 "오래된 것을 비우고 새것을 받는" 정상 LRU 동작이 유지된다.

---

## 2. 무엇을 구현했나 (설계안 C2)

핵심 아이디어: **사용량이 워터마크를 상향 돌파하는 그 순간** 컨트롤러를 깨운다.
단, 폴링을 완전히 없애지 않고 **안전 플로어 타임아웃**으로 남긴다(신호 유실·격리모드 대비).

변경은 딱 두 파일.

### (a) `lmcache/v1/distributed/l2_adapters/base.py` — 깨우기 훅 + edge 감지

어댑터 베이스에 "사용량이 임계 바이트를 넘으면 콜백을 부르는" 훅을 추가:

```python
def set_eviction_wake(self, threshold_bytes: int, wake: Callable[[], None]) -> None:
    """컨트롤러가 와이어링 시 1회 호출. usage가 threshold_bytes를 상향 돌파하면 wake()를 부른다."""
    self._eviction_wake_threshold_bytes = threshold_bytes
    self._eviction_wake = wake
```

그리고 store가 회계에 반영되는 자리(`_notify_keys_stored`)에서 **edge(아래→위 전이)** 를 감지:

```python
with self._usage_lock:
    prev_total = self._total_bytes_used
    self._total_bytes_used += total_delta
    new_total = self._total_bytes_used
    ...
# 락 밖에서, 임계를 "방금 넘었을 때만" 깨운다 (계속 넘어 있으면 다시 안 부름 = 디바운스)
wake = self._eviction_wake
threshold = self._eviction_wake_threshold_bytes
if wake is not None and threshold > 0 and prev_total < threshold <= new_total:
    wake()
```

- **edge-triggered**: `prev < threshold <= new`일 때만 발화 → 워터마크 위에서 쏟아지는 store마다
  부르지 않고 *넘는 순간 한 번*만. eviction이 사용량을 내리면 자연히 재무장된다.
- **훅 미설정이면 아무 일도 없음** → 기존 동작 그대로(하위호환).

### (b) `lmcache/v1/distributed/storage_controllers/eviction_controller.py` — 이벤트 루프

컨트롤러에 깨우기용 `Event`를 두고, 폴링을 "이벤트 + 플로어 타임아웃 대기"로 교체:

```python
_POLL_FLOOR_SECONDS: float = 1.0          # 신호를 놓쳐도 최소 이 간격마다 한 번은 돈다

def __init__(self, ...):
    ...
    self._wake = threading.Event()
    self._install_eviction_wakes()        # 각 어댑터에 훅 배선

def _install_eviction_wakes(self) -> None:
    for state in self._adapter_states:
        if state.eviction_policy.support_isolation:   # per-cache_salt 격리모드는 제외(§6)
            continue
        capacity = state.adapter.get_usage().total_capacity_bytes
        if capacity <= 0:                              # 용량 모르면 임계도 없음
            continue
        threshold = int(state.eviction_config.trigger_watermark * capacity)
        state.adapter.set_eviction_wake(threshold, self._wake.set)

def _eviction_loop(self):
    while not self._stop_flag.is_set():
        self._wake.wait(timeout=self._POLL_FLOOR_SECONDS)   # 깨우면 즉시, 아니면 1초마다
        self._wake.clear()
        for state in self._adapter_states:
            self._check_and_evict(state)

def stop(self):
    self._stop_flag.set()
    self._wake.set()        # 대기 풀어서 즉시 종료
    self._thread.join()
```

---

## 3. 어떻게 동작하나 (흐름)

```
store 완료
   └─ base._notify_keys_stored: total_bytes_used += size
        └─ usage가 워터마크(threshold)를 "방금" 넘었나? (edge)
             └─ 예 → self._wake.set()  (컨트롤러의 Event)
                       └─ eviction 루프의 wait()가 즉시 반환
                            └─ _check_and_evict → LRU victim 회수 → 슬롯 확보
   (계속 위에 머무르면 더는 안 부름. eviction이 아래로 내리면 다음 돌파 때 재무장)
```

- 평소(워터마크 아래): 이벤트 안 옴 → 루프는 1초 플로어로 한가하게 돈다.
- 압력 발생: 넘는 순간 ~ms 내 회수 시작 → 슬롯이 차기 전에 비워 신규 store가 계속 성공.
- 신호를 혹시 놓쳐도 1초 플로어가 받쳐줌 → 완전 이벤트화의 위험(영영 안 깸)을 회피.

---

## 4. 왜 "성능 향상"인가

성능 이득의 사슬은 store 속도가 아니라 **캐시가 옳은 데이터를 들고 있느냐**에서 온다:

```
store 거부 → 그 KV 청크가 L2에 안 남음
          → 미래 요청이 그 청크에서 lookup miss
          → 그 토큰들의 prefill을 GPU로 재계산
          → TTFT↑, GPU 낭비, throughput↓
```

역으로 거부가 줄면(재사용이 있을 때) miss·재계산이 줄어 TTFT가 내려간다. 또 LMCache 로드는
**prefix-only**라 중간 청크 하나만 빠져도 그 뒤 prefix 전체의 재사용이 깨진다 — 즉 포화 중
최신 청크를 드롭하는 폴링의 행동이 특히 치명적이다. 이벤트 트리거는 이 "anti-LRU 퇴화"를 막아
**최신·핫 KV가 상주하게** 만든다.

---

## 5. 검증 (실측)

| 단계 | 측정 | 결과 |
|------|------|------|
| L1 | 단위 테스트 11개 (edge 동작/재무장/하위호환/와이어링/stop) | ✅ 전부 통과 + 회귀 통과 |
| L2 | 반응 지연: breach→워터마크 아래 복귀 (이벤트 ON vs 폴링 OFF) | **~1000ms → ~1.07ms (≈937x)** |
| L3 | raw_block 거부율: 3초 버스트, 슬롯 50 (실제 ext+adapter+controller) | **99.90% → 0.07%** (거부 96,447→63) |
| L3.5 | 재사용+drift 워크로드의 L2 hit rate (동일 시퀀스 replay) | **0.56% → 40.95% (+40.4pp)**, miss 31,822→18,895 |
| L4 | 실 vLLM e2e TTFT/hit rate | ⬜ 진행 중 (별도 venv 셋업 중) |

**정직한 단서 (과대해석 금지):**
- 이득은 **포화 + 재사용**이 동시에 있을 때만 발현. 가벼운 부하(워터마크 미도달)면 폴링과 동일·무해.
- L2/L3/L3.5의 극적인 격차는 **고압력 합성 워크로드** 기준. 일반화: *OFF는 신규 admit을
  `evict_batch / poll_interval`(초당)로 캡, ON은 그 캡 제거.* 실 store rate가 캡 이하면 차이 작다.
- 아직 **adapter 레벨** 측정(실 vLLM 아님). 실제 TTFT 수치는 L4 필요.

(벤치 스크립트: `bench_reaction.py`, `bench_putfail.py`, `bench_hitrate.py` — 모두 재현 가능.
자세한 검증 사다리·단서는 [`02-verification.md`](02-verification.md).)

---

## 6. 범위와 안전성

- **MP 모드 한정.** `L2AdapterInterface` + `L2EvictionController`는 MP/distributed 경로(업스트림
  `[MP]` 태그 영역). non-MP raw_block(`RustRawBlockBackend`, `StoragePluginInterface`)은 이 컨트롤러를
  안 쓰므로 **영향 없음**.
- **하위호환.** `set_eviction_wake`를 안 부르면 기존 1초 폴링 동작 그대로.
- **격리(per-cache_salt) 모드 제외(v1).** 거긴 quota 기반이라 edge 감지가 어려움 → 플로어 폴링 유지.
- **과다 축출 없음.** 루프는 회수 전 `usage >= watermark`를 재검증하므로 잘못된 깨우기는 그냥 no-op.
- **비용 거의 0.** raw_block의 delete는 in-memory(디바이스 I/O 없음)라 이벤트로 더 자주 돌아도 싸다.

---

## 7. 변경 파일 · 테스트 · 브랜치

**소스 (2 파일):**
- `lmcache/v1/distributed/l2_adapters/base.py` — `set_eviction_wake` + `_notify_keys_stored` edge 감지
- `lmcache/v1/distributed/storage_controllers/eviction_controller.py` — `_wake` Event, 와이어링, 루프, stop

**테스트 (2 파일, 11 케이스):**
- `tests/v1/distributed/test_l2_adapter_base.py::TestEvictionWake` (6) — edge 발화/디바운스/재무장/미설정/임계0/미달
- `tests/v1/distributed/test_l2_eviction_watermark_trigger.py` (5) — 와이어링, 격리 제외, zero-cap 제외, stop 즉시, 플로어 전 eviction

**브랜치/커밋:** `l2-eviction-watermark-trigger` / `b0e0708f [MP] l2 eviction: event-driven watermark trigger` (DCO 서명, 미push)

---

## 8. 더 깊이 보려면

다음 자료는 개인 작업 공간(`dev/analysis/eviction-watermark-trigger/`, git 미추적)에 있다:

- 설계 상세 `01-design.md` — C1/C2 비교, 정합성 엣지케이스, 스코프
- 검증 상세 `02-verification.md` — 검증 사다리(L1~L4)와 벤치 결과·단서
- 재현용 벤치: `bench_reaction.py`(L2), `bench_putfail.py`(L3), `bench_hitrate.py`(L3.5)
