#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NCBI Genome Manager — FastAPI 应用入口。

启动方式::

    cd /home/msadmin/github/ncbi_genome_manager
    uvicorn genome_manager.main:app --host 0.0.0.0 --port 8000 --reload
"""
from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Form, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware

from .database import (
    init_db, get_db,
    db_is_admin_configured, db_set_admin_credentials, db_verify_admin_credentials,
)
from .routers import api as api_router
from .routers import sse as sse_router
from .tasks import auto_update_scheduler, task_queue

# ─────────────────────────────────────────────────────────────
# 路径常量
# ─────────────────────────────────────────────────────────────
BASE_DIR   = Path(__file__).parent
STATIC_DIR = BASE_DIR / "static"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# 生命周期
# ─────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Initializing database…")
    await init_db()
    logger.info("Starting task queue worker…")
    await task_queue.start()
    logger.info("Starting auto-update scheduler…")
    await auto_update_scheduler.start()
    logger.info("NCBI Genome Manager is ready.")
    yield
    logger.info("Shutting down.")


# ─────────────────────────────────────────────────────────────
# 应用实例
# ─────────────────────────────────────────────────────────────
app = FastAPI(
    title="NCBI Genome Manager",
    description="可视化管理下载/更新 NCBI 基因组数据",
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
    lifespan=lifespan,
)

# CORS — 开发环境允许所有来源；生产环境应限制为具体域名
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Session 中间件（用于管理员登录状态）
_SECRET_KEY = os.environ.get("NGM_SECRET_KEY", "ngm-change-me-in-production")
app.add_middleware(SessionMiddleware, secret_key=_SECRET_KEY, session_cookie="ngm_session")

# ─────────────────────────────────────────────────────────────
# 路由注册
# ─────────────────────────────────────────────────────────────
app.include_router(api_router.router)
app.include_router(sse_router.router)

# 静态资源（CSS / JS / 图片等）
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# ─────────────────────────────────────────────────────────────
# HTML 页面路由
# ─────────────────────────────────────────────────────────────

@app.get("/setup", include_in_schema=False)
async def setup_page():
    async with get_db() as db:
        if await db_is_admin_configured(db):
            return RedirectResponse("/login", status_code=302)
    return FileResponse(str(STATIC_DIR / "setup.html"))


@app.post("/setup", include_in_schema=False)
async def do_setup(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
):
    async with get_db() as db:
        if await db_is_admin_configured(db):
            return RedirectResponse("/login", status_code=302)
        await db_set_admin_credentials(db, username, password)
    return RedirectResponse("/login?setup=1", status_code=302)


@app.get("/login", include_in_schema=False)
async def login_page():
    return FileResponse(str(STATIC_DIR / "login.html"))


@app.post("/login", include_in_schema=False)
async def do_login(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
):
    async with get_db() as db:
        valid = await db_verify_admin_credentials(db, username, password)
    if valid:
        request.session["authenticated"] = True
        next_url = request.query_params.get("next", "/admin")
        return RedirectResponse(next_url, status_code=302)
    return RedirectResponse("/login?error=1", status_code=302)


@app.get("/logout", include_in_schema=False)
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/login", status_code=302)


@app.get("/admin", include_in_schema=False)
async def admin_page(request: Request):
    async with get_db() as db:
        if not await db_is_admin_configured(db):
            return RedirectResponse("/setup", status_code=302)
    if not request.session.get("authenticated"):
        return RedirectResponse("/login?next=/admin", status_code=302)
    return FileResponse(str(STATIC_DIR / "admin.html"))


@app.get("/", include_in_schema=False)
async def index(request: Request):
    async with get_db() as db:
        if not await db_is_admin_configured(db):
            return RedirectResponse("/setup", status_code=302)
    if not request.session.get("authenticated"):
        return RedirectResponse("/login?next=/admin", status_code=302)
    return RedirectResponse("/admin", status_code=302)


@app.get("/stats", include_in_schema=False)
async def stats_page():
    return FileResponse(str(STATIC_DIR / "stats.html"))


@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return FileResponse(str(STATIC_DIR / "favicon.ico"))
