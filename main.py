"""服务入口：装配 FastAPI + 本地异步任务队列。"""

from __future__ import annotations

import uvicorn

from jobqueue.api import create_app

app = create_app()


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
