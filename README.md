# async-retry-job-queue-python-bootstrap

在原有 FastAPI 服务上增加的一套**本地可跑**的后台异步任务处理能力：
幂等提交、退避重试、死信、并发上限、优雅关停与崩溃恢复。
**不依赖任何外部消息中间件或额外服务**——持久化只用本地 SQLite，调度只用进程内 asyncio。

## 快速开始

```bash
uv sync
uv run uvicorn main:app --port 8000
```

```bash
# 幂等提交（同一 business_key 多提交几次：job_id 不变、created 仅首次为 true）
curl -s -X POST localhost:8000/jobs -H 'content-type: application/json' \
  -d '{"business_key":"demo-1","job_type":"fail_then_succeed",
       "payload":{"key":"demo-1","fail_times":2},"max_attempts":4}'

# 查询状态与每次尝试的历史
curl -s localhost:8000/jobs/by-key/demo-1 | python -m json.tool
```

## 测试

```bash
uv run pytest -q          # 16 个用例：幂等/并发/重试死信/关停恢复 + HTTP 接口
```

## 代码结构

| 路径 | 职责 |
| --- | --- |
| `jobqueue/models.py` | 状态机枚举、死信原因、Job/RetryPolicy 模型 |
| `jobqueue/storage.py` | SQLite：UNIQUE 幂等键、事务内原子抢占、状态守卫、启动恢复 |
| `jobqueue/retry.py` | 指数退避 + 全抖动 |
| `jobqueue/errors.py` | 异常分类与可解释的失败判定 |
| `jobqueue/registry.py` | job_type -> 处理器注册表 |
| `jobqueue/queue.py` | worker 池、超时、重试、死信、优雅关停 |
| `jobqueue/handlers.py` | 内置演示任务 |
| `jobqueue/api.py` / `main.py` | HTTP 接口与启动入口 |
| `tests/` | 场景测试（含子进程模拟 kill -9 恢复） |
| `docs/ARCHITECTURE.md` | 状态机、重试/死信规则、并发与关停行为、本地验证详解 |

环境变量：`JOBQUEUE_DB`（默认 `.data/jobs.db`）、`JOBQUEUE_WORKERS`（默认 4）、
`JOBQUEUE_SHUTDOWN_TIMEOUT`（默认 15s）。
