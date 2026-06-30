#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Server-Sent Events (SSE) 路由 — 实时推送任务日志。
Phase 3 将完整实现；本文件已提供可用的轮询式 SSE 端点。
"""
from __future__ import annotations

import asyncio
import json

from fastapi import APIRouter, Query
from fastapi.responses import StreamingResponse

from ..database import db_get_logs, db_get_task, get_db

router = APIRouter(prefix="/api", tags=["sse"])

_SSE_POLL_INTERVAL = 1.0   # 每秒轮询一次数据库
_SSE_DONE_STATUSES = {"done", "failed", "cancelled"}


@router.get(
    "/tasks/{task_id}/logs/stream",
    summary="任务日志实时流 (SSE)",
    response_class=StreamingResponse,
)
async def stream_task_logs(
    task_id: int,
    after_id: int = Query(0, ge=0, description="仅返回 id 大于此值的日志（用于续订 SSE 而不重放历史）"),
) -> StreamingResponse:
    """以 Server-Sent Events 实时推送任务日志。

    客户端示例::

        const es = new EventSource(`/api/tasks/${taskId}/logs/stream`);
        es.onmessage = e => {
            const msg = JSON.parse(e.data);
            if (msg.event === 'done') { es.close(); }
        };
    """

    async def event_generator():
        last_id = after_id
        while True:
            try:
                async with get_db() as db:
                    task = await db_get_task(db, task_id)
                    if task is None:
                        yield _sse_event({"event": "error", "detail": "Task not found"})
                        return

                    logs = await db_get_logs(db, task_id, after_id=last_id)

                for log in logs:
                    last_id = log["id"]
                    yield _sse_event({
                        "id":         log["id"],
                        "content":    log["content"],
                        "created_at": log["created_at"],
                    })

                if task["status"] in _SSE_DONE_STATUSES:
                    yield _sse_event({"event": "done", "status": task["status"]})
                    return

                # 发送心跳，防止代理/浏览器超时断连
                yield ": heartbeat\n\n"

            except asyncio.CancelledError:
                return
            except Exception as exc:
                yield _sse_event({"event": "error", "detail": str(exc)})
                return

            await asyncio.sleep(_SSE_POLL_INTERVAL)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control":    "no-cache",
            "X-Accel-Buffering": "no",
            "Connection":        "keep-alive",
        },
    )


def _sse_event(data: dict) -> str:
    """将字典序列化为 SSE data 行。"""
    return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"
