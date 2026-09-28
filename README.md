# async-retry-job-queue（本地异步任务队列）

在 FastAPI 服务内嵌的一套**本地即可运行**的后台异步任务处理能力：
幂等提交、退避重试、死信、并发上限、硬超时与优雅关停。
只依赖标准库（`sqlite3` / `multiprocessing` / `threading`）与 FastAPI，
**不需要任何外部消息中间件（Redis/RabbitMQ 等）或额外部署的服务**。

## 架构概览

```
HTTP (FastAPI)
   │ submit / get
   ▼
JobQueue（进程内门面）
   │  一个调度线程：周期性 claim_due
   ▼
有界 ThreadPoolExecutor（max_concurrency = 硬并发上限）
   │  每个槽位 fork 一个隔离子进程执行处理器
   ▼
JobStore（SQLite，WAL）：提交幂等 + 原子抢占 + 乐观锁状态写回 + 崩溃恢复
```

- **为什么子进程执行**：超时任务可能死循环或卡在 C 扩展里，线程无法被
  强制终止；子进程可以 `terminate/kill`，单个坏任务最多占一个槽位到
  drain/timeout，**不会拖垮其他任务**。
- **处理器约定**：必须是**模块顶层函数**（可 pickle / 可 import），
  payload 可 pickle、返回值建议可 JSON 序列化。处理器抛
  `jobqueue.NonRetryableError` 表示不可重试。

## 任务状态机

状态单向流转（带版本号乐观锁，禁止回退/覆盖）：

```
PENDING ──claim──▶ RUNNING ──成功──────────────▶ SUCCEEDED   （终态）
                     │
                     ├──普通失败/超时，仍有重试预算──▶ RETRY_WAIT
                     │                                 │
                     │                           run_after 到期
                     │                                 └──claim──▶ RUNNING
                     │
                     ├──普通失败/超时，重试预算耗尽──▶ DEAD        （终态）
                     └──不可重试错误───────────────▶ DEAD        （终态）

优雅关停 drain 截止仍在跑：RUNNING ──▶ RETRY_WAIT（attempts 回退，不计重试）
```

| 状态 | 含义 |
|---|---|
| `PENDING` | 已提交，等待首次调度 |
| `RUNNING` | 已被唯一执行者抢占（`claim_due` 原子置位），正在子进程执行 |
| `RETRY_WAIT` | 失败后退避等待，`run_after` 到期重新可调度；也是停机中断的落点 |
| `SUCCEEDED` | 成功，终态 |
| `DEAD` | 死信，终态，带可区分的 `failure_reason` |

- 一次任务总尝试次数 = `1 + max_retries`（首发 + 重试）。
- `attempts` 在抢占时 +1；**被优雅关停中断的尝试会回退**，不消耗重试预算。
- 终态单调：终态后用同一 `business_key` 重复提交只会读到终态，不会被重新激活。
- 每次流转 `version += 1`；写回带 `WHERE version=?`，冲突结果被拒绝（状态不串任务）。

## 幂等提交规则

- 幂等键是 `business_key`（数据库 UNIQUE 约束 + `INSERT OR IGNORE`）。
- 同键重复提交（无论任务处于 PENDING/RUNNING/RETRY_WAIT/终态）：
  返回**同一个任务 id 与当前状态**，HTTP `202` 且 `created=false`，
  **既不产生第二次执行，也不报错丢弃**。
- 8 个线程同时提交同一键，数据库层也只有一条记录（测试
  `test_concurrent_submit_same_business_key_single_job` 覆盖）。

## 重试与死信规则

| 执行结果 | 有重试预算 | 无重试预算 |
|---|---|---|
| 普通异常（任意 `Exception`） | `RETRY_WAIT`，退避后重试 | `DEAD / RETRIES_EXHAUSTED` |
| 执行超时（子进程被杀） | `RETRY_WAIT`，退避后重试 | `DEAD / TIMEOUT_RETRIES_EXHAUSTED` |
| `NonRetryableError` | 不重试，直接 `DEAD / NON_RETRYABLE` | 同左 |

三种死信原因（`failure_reason`）可直接区分，`last_error` 记录最后一次
错误文本（含异常类型），全部可通过 `GET /jobs/{id}` 查询。

退避策略可配置（`jobqueue.backoff`）：

- `ExponentialBackoff(base, factor, cap, rng)`：全抖动指数退避
  `delay = uniform(0, min(cap, base * factor**(n-1)))`；
- `FixedBackoff(delay)`：固定间隔；`NoBackoff()`：立即重试（测试用）。
- 自定义实现 `Backoff.delay_for(retry_index)` 即可。

## 并发与超时

- `max_concurrency` 是硬上限：调度线程只按空闲槽位 `claim_due`，
  抢占用单条 `UPDATE ... RETURNING` 原子完成，多线程/多进程下同一任务
  不可能被执行两次。
- 每个任务可单独给 `timeout_seconds`；超时后子进程先 `terminate` 再
  `kill`，执行槽随即释放，后续任务立刻补位。

## 优雅关停行为

收到 `SIGTERM`/`SIGINT`（uvicorn lifespan）或调用
`queue.shutdown(timeout)` / `shutdown_blocking(timeout)`：

1. **停止接收新任务**：`submit` 抛 `QueueClosedError`，HTTP 返回 `503`；
   调度线程停止抢占（未开始的任务保持 `PENDING`）。
2. **收尾在跑任务**：在 `timeout`（drain 预算）内让任务自然跑完，
   成功/失败/任务自身超时都按真实结果写回终态或退避。
3. **drain 截止仍未结束的任务**：杀掉子进程，置 `RETRY_WAIT`、
   `attempts` 回退、记录 `ShutdownInterrupted`，**不判失败、不消耗重试**。
4. 未完成任务（`PENDING`/`RETRY_WAIT`）本来就持久化在 SQLite；
   下次进程 `start()` 时，上次残留的 `RUNNING` 也会被统一重置为
   `RETRY_WAIT` 后继续执行——**任务不丢失、状态不错乱**。

## HTTP 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| `POST` | `/jobs` | 提交任务，body：`task_type`、`business_key`、`payload`、可选 `max_retries`、`timeout_seconds`；返回任务全量字段 + `created` |
| `GET` | `/jobs/{id}` | 按任务 id 查询状态 |
| `GET` | `/jobs?business_key=...` | 按业务键查询 |
| `GET` | `/health` | `running` / `draining`（关停中） |

## 本地验证

```bash
uv sync

# 1) 全量单测（覆盖：重复提交、并发抢占、重试耗尽、死信区分、停机收尾与重启恢复）
uv run pytest -v -s          # -s 可实时看到带 job_id/attempts/verdict 的判定日志

# 2) 起本地服务（默认 ./jobqueue.db 文件持久化）
uv run uvicorn main:app --host 127.0.0.1 --port 8000

# 3) 提交与幂等验证（第二次返回同一 id、created=false、当前状态）
curl -s -X POST localhost:8000/jobs -H 'Content-Type: application/json' \
  -d '{"task_type":"echo","payload":{"order":1001},"business_key":"order-1001"}'
curl -s -X POST localhost:8000/jobs -H 'Content-Type: application/json' \
  -d '{"task_type":"echo","payload":{"order":1001},"business_key":"order-1001"}'
curl -s "localhost:8000/jobs?business_key=order-1001"

# 4) 优雅关停：kill -TERM <pid>，观察日志 verdict=draining -> closed；
#    重启服务后未完成任务会自动继续执行
```

可用环境变量：`JOBQUEUE_DB`（默认 `jobqueue.db`，设为 `:memory:` 即纯内存）、
`JOBQUEUE_CONCURRENCY`、`JOBQUEUE_MAX_RETRIES`、`JOBQUEUE_TIMEOUT`、
`JOBQUEUE_SHUTDOWN_TIMEOUT`。

## 代码结构

| 文件 | 职责 |
|---|---|
| `jobqueue/models.py` | 状态机、`Job`、错误类型与死信原因 |
| `jobqueue/backoff.py` | 退避策略 |
| `jobqueue/storage.py` | SQLite 仓储：幂等插入、原子抢占、乐观锁写回、停机重置 |
| `jobqueue/executor.py` | 隔离子进程执行、硬超时、关停中断 |
| `jobqueue/queue.py` | 调度循环、重试判定、并发槽位、优雅关停 |
| `jobqueue/api.py` | FastAPI 装配与 lifespan |
| `jobqueue/handlers.py` | 内置演示处理器 |
| `tests/` | 幂等 / 并发 / 重试死信 / 关停恢复 / HTTP 测试 |

## 边界与说明

- 持久化面向**单进程服务**（一个服务进程持有一个 SQLite 文件）；多实例
  水平部署不属于本方案范围，但 SQLite 层的抢占/乐观锁保证同库连接下不会重复执行。
- 执行隔离使用 `fork`（Linux/macOS 默认）；处理器仍建议写成可导入的
  顶层函数。任务结果通过临时 JSON 文件从子进程回传。
