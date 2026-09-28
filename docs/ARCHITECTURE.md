# 本地异步任务队列：设计与行为说明

一套**无需任何外部消息中间件 / 无需额外部署服务**的后台异步任务处理能力。
持久化基于本地 SQLite 文件，调度基于进程内 asyncio worker 池，对外通过
FastAPI 提供提交与查询接口。

- 存储：`jobqueue/storage.py`（SQLite，WAL 模式，默认 `.data/jobs.db`）
- 调度：`jobqueue/queue.py`（dispatcher + 固定 worker 池 + 单次超时 + 退避重试）
- 接口：`jobqueue/api.py`（`POST /jobs`、`GET /jobs/{id}`、`GET /jobs/by-key/{key}`）
- 示例任务：`jobqueue/handlers.py`

## 1. 任务状态机

状态集合：`PENDING`、`RUNNING`、`RETRY_WAIT`、`SUCCEEDED`、`DEAD`

```
                         提交（business_key 唯一）
                                │
                                ▼
                         ┌────────────┐  dispatcher 抢占（claim，事务内原子）
                         │  PENDING   │───────────────┐
                         └────────────┘                ▼
                              ▲                    ┌──────────┐  handler 正常返回
                              │                    │ RUNNING  │──────────────► SUCCEEDED（终态）
                              │ 退避到期重新抢占     └──────────┘
                              │                       │  │  │
              进程关停/崩溃恢复 │                       │  │  ├─ 抛出 NonRetryable ─────────────► DEAD（NON_RETRYABLE_ERROR）
                              │                       │  │  ├─ 可重试异常/超时且 attempt < max  │
                              │                       │  │  │   退避 not_before 后              │
                              └───────────────────────┘  │  ▼                                    │
                                    （RUNNING 也可        └─► RETRY_WAIT                          │
                                     在关停后回退）            │                                 │
                                                              ├─ 可重试异常，第 max 次失败 ─────►│ DEAD（RETRIES_EXHAUSTED）
                                                              └─ 超时，第 max 次尝试 ───────────►┘ DEAD（TIMEOUT_EXHAUSTED）
```

**单调性保证**

- 所有状态流转 SQL 都带前置状态守卫，例如
  `UPDATE ... SET status='SUCCEEDED' WHERE id=? AND status='RUNNING' AND attempts=?`。
  终态（SUCCEEDED/DEAD）永远不可能被后续写入覆盖，状态不允许回退。
- `attempts` 表示「已开始的尝试次数」，只增不减。
- 每次尝试都会向 `attempts` 历史表追加一条记录（SUCCESS / RETRYABLE_ERROR /
  NON_RETRYABLE_ERROR / TIMEOUT / SHUTDOWN_INTERRUPTED），**全流程可追溯**，
  查询接口返回完整 history。

## 2. 幂等提交

- 以调用方提供的 **business_key 作为业务幂等键**，数据库表上有 UNIQUE 约束兜底。
- 重复提交（无论任务在 PENDING / RUNNING / RETRY_WAIT / SUCCEEDED / DEAD）：
  - 不产生第二个任务、不触发第二次执行、也不报错丢弃；
  - 返回同一个 `job_id`、`created=false` 与任务**当前**最新状态。
- 即使 20 个协程并发提交同一 business_key，UNIQUE 冲突 + 「冲突即读既有行」
  的逻辑保证只有一个任务落库（见 `tests/test_idempotency.py`）。

## 3. 重试与退避

每次尝试独立配置（`RetryPolicy`，可逐任务覆盖）：

| 参数 | 默认 | 含义 |
| --- | --- | --- |
| `max_attempts` | 3 | 最大尝试次数（含首次） |
| `attempt_timeout` | 10s | **单次尝试**超时，不影响其他任务 |
| `backoff_base` | 0.05s | 退避基数 |
| `backoff_factor` | 2.0 | 指数因子 |
| `backoff_max` | 5.0s | 退避上限 |

退避公式（指数 + 全抖动，避免惊群）：

```
delay = random(0, min(backoff_max, backoff_base * factor ** (failed_attempts - 1)))
```

失败后任务进入 RETRY_WAIT 并写入 `not_before = now + delay`，dispatcher
只会抢占 `not_before <= now` 的任务。

## 4. 错误分类与死信原因（可区分）

判定依据在 `jobqueue/errors.py`，每次失败都会写入 `last_error_type`、
`last_error_message`（含中文判定说明）和 history：

| 执行情况 | attempt 结果 | 耗尽后的 failure_reason |
| --- | --- | --- |
| 抛 `jobqueue.errors.NonRetryable`（或子类） | NON_RETRYABLE_ERROR | **NON_RETRYABLE_ERROR**（首次即死信，不再重试） |
| 超过 `attempt_timeout` | TIMEOUT | **TIMEOUT_EXHAUSTED** |
| 其他任何异常 | RETRYABLE_ERROR | **RETRIES_EXHAUSTED** |
| 关停信号中断在途尝试 | SHUTDOWN_INTERRUPTED | 不进死信：回退 RETRY_WAIT，重启后续跑 |

三类死信原因互不相同，查询接口直接返回 `failure_reason`，便于告警与排障。

## 5. 并发与隔离

- dispatcher 用 `asyncio.Semaphore(max_workers)` 控制并发：**先拿到名额再抢占**，
  没有空闲 worker 就不产生新执行体，并发数严格不超过上限。
- 抢占在单条 SQLite 事务（`BEGIN IMMEDIATE` + 条件 UPDATE）内完成，
  天然互斥：一个任务同时只会被一个执行者拿到。
- 每个任务在独立 asyncio 任务中运行，`asyncio.wait_for` 施加单次超时；
  某个任务卡住或持续失败，只影响它自己，其他任务照常调度
  （见 `test_timeout_job_does_not_block_others`）。

## 6. 优雅关停与崩溃恢复

`POST /jobs` 的处理在 FastAPI lifespan 内运行；收到 SIGTERM/SIGINT 或
uvvicorn 停止时进入 shutdown 阶段调用 `queue.stop()`：

1. 在与提交共用的 DB 锁内翻转 `_accepting=false` 并设置关停事件——
   **此后新提交一律返回 503（QueueShuttingDown），且不会出现「检查通过但没落库」**；
2. dispatcher 在一个 poll 间隔内停止抢占新任务；
3. 正在执行的尝试被协作式取消，`_execute` 的 SHUTDOWN_INTERRUPTED 分支
   统一把任务落库为 RETRY_WAIT（`run_after_shutdown=1`、`not_before=now`，
   追加中断历史）。**在途任务不丢失、不会停在 RUNNING、终态不被覆盖**；
4. 原本就在 RETRY_WAIT/PENDING 的任务原样保留。

**进程被 kill -9 / 机器掉电**时来不及收尾，可能有任务停留在 RUNNING：
下次启动 `recover_on_startup` 会把全部残留 RUNNING 回收为 RETRY_WAIT
（立即到期、追加 SHUTDOWN_INTERRUPTED 历史、`run_after_shutdown=1`），
由新进程重新执行；中断的尝试保留计数，attempts 单调递增。恢复在启动时
只执行一次。

## 7. HTTP 接口

| 方法与路径 | 说明 |
| --- | --- |
| `GET /` | 服务信息与可用 job_type |
| `POST /jobs` | 幂等提交。body：`business_key`、`job_type`、`payload`、可选 `max_attempts`、`attempt_timeout`；返回 `202 {job, created}`；重复提交 `created=false` |
| `GET /jobs/{job_id}` | 查询状态、attempts、failure_reason、last_error_*、result、history |
| `GET /jobs/by-key/{business_key}` | 按业务键查询 |
| 关停期 `POST /jobs` | 返回 `503`，任务不落库 |
| 未知 job_type | 返回 `400` |

## 8. 本地验证

```bash
# 启动（默认 .data/jobs.db，4 个 worker；可用环境变量覆盖）
uv run uvicorn main:app --port 8000
# 或：JOBQUEUE_WORKERS=8 JOBQUEUE_DB=.data/jobs.db uv run python main.py

# 提交（幂等：多执行几次，created 只会在第一次为 true，job_id 不变）
curl -s -X POST localhost:8000/jobs -H 'content-type: application/json' \
  -d '{"business_key":"demo-1","job_type":"fail_then_succeed",
       "payload":{"key":"demo-1","fail_times":2},"max_attempts":4}'

# 查询（观察 status: PENDING/RUNNING -> RETRY_WAIT -> SUCCEEDED，history 递增）
curl -s localhost:8000/jobs/by-key/demo-1 | python -m json.tool

# 死信原因对比：always_retryable -> RETRIES_EXHAUSTED；
# always_non_retryable -> NON_RETRYABLE_ERROR；slow 超时 -> TIMEOUT_EXHAUSTED
curl -s -X POST localhost:8000/jobs -H 'content-type: application/json' \
  -d '{"business_key":"dead-1","job_type":"always_non_retryable"}'
```

### 运行测试

```bash
uv run pytest -q                       # 全部 16 个用例
uv run pytest tests/test_shutdown.py -q -o log_cli=true   # 观察关停日志
```

测试覆盖：重复提交（含并发重复）、抢占原子性、并发上限、退避后成功、
重试耗尽、不可重试错误、三类死信原因可区分、超时任务隔离、关停拒绝新提交、
关停在途收尾与等待态保留、子进程模拟 kill -9 后的重启恢复。
日志中打印 job_id、重试次数（attempts/max_attempts）与判定依据。

### 内置演示任务类型

| job_type | 行为 |
| --- | --- |
| `succeed` | 恒成功，回显 payload |
| `fail_then_succeed` | payload 传 `{"key":"x","fail_times":2}`：前 2 次抛可重试错误后成功 |
| `always_retryable` | 恒失败（可重试），用于 RETRIES_EXHAUSTED |
| `always_non_retryable` | 首次即不可重试，用于 NON_RETRYABLE_ERROR |
| `slow` | payload `{"seconds":30}`：睡眠，用于超时/关停演示 |
