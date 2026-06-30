#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
REST API 路由。
所有路由前缀 /api/，按资源分组：tasks、configs、genomes、stats。
"""
from __future__ import annotations

import asyncio
import json
import secrets
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Literal, Optional

from fastapi import APIRouter, Body, HTTPException, Query, UploadFile, File
from fastapi.responses import StreamingResponse
from fastapi.responses import JSONResponse

from genome_downloader.utils import non_ref_md5_path, non_ref_metadata_path, ref_md5_path

from ..database import (
    db_add_log,
    db_count_genomes,
    db_count_tasks,
    db_create_task,
    db_create_taxon_config,
    db_delete_tasks,
    db_delete_taxon_config,
    db_get_steps_due_for_update,
    db_get_genome_groups,
    db_get_logs,
    db_get_task,
    db_get_taxon_config,
    db_list_genomes,
    db_list_tasks,
    db_list_taxon_configs,
    db_update_task,
    db_update_taxon_config,
    get_db,
)
from ..models import TaskCreate, TaxonConfigCreate
from ..tasks import task_queue

router = APIRouter(prefix="/api")

# 隐藏仓库目录名（类似 .git）
_NGM_DIR = ".ngm"

# 写入 .ngm/config.json 时排除的字段（敏感 / 纯 DB 内部字段）
_CONFIG_EXCLUDE = {
    "api_key", "id", "created_at", "updated_at",
    "pending_count", "pending_format_count", "predownload_count",
    "last_auto_updated",
    "next_check_at", "next_download_at", "next_process_at", "next_predownload_at",
}


def _init_genome_repo(config: dict) -> None:
    """在 genome_dir 下初始化 .ngm/ 隐藏目录（类似 git init）。

    - 若 .ngm/ 不存在则创建并写入 config.json
    - 若已存在则只更新 config.json（保留其余文件）
    - api_key 等敏感字段不写入

    .ngm/ 用途：
      config.json   — 当前配置快照（方便 CLI / 脚本直接读取）
      (预留)         — 未来可在此存放计划文件、日志等配置相关文件
    """
    genome_dir = config.get("genome_dir")
    if not genome_dir:
        return
    ngm_dir = Path(genome_dir) / _NGM_DIR
    ngm_dir.mkdir(parents=True, exist_ok=True)

    safe_cfg = {k: v for k, v in config.items() if k not in _CONFIG_EXCLUDE}
    safe_cfg["initialized_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    cfg_file = ngm_dir / "config.json"
    cfg_file.write_text(
        json.dumps(safe_cfg, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


@router.post('/upload/icon', tags=['configs'], summary='上传自定义图标（PNG/JPEG/SVG/GIF）')
async def upload_icon(file: UploadFile = File(...)) -> dict:
    """接收单个图片文件，保存到 static/uploads 并返回可用于前端的访问路径。

    返回字段：{"path": "/static/uploads/xxx.png"}
    """
    ALLOWED = {"image/png", "image/jpeg", "image/svg+xml", "image/gif"}
    if file.content_type not in ALLOWED:
        raise HTTPException(status_code=400, detail=f"不支持的文件类型: {file.content_type}")

    # 保存到项目主静态目录： genome_manager/static/uploads
    uploads_dir = Path(__file__).resolve().parent.parent / "static" / "uploads"
    uploads_dir.mkdir(parents=True, exist_ok=True)

    suffix = Path(file.filename).suffix or ""
    name = secrets.token_hex(12) + suffix
    dest = uploads_dir / name
    try:
        with dest.open("wb") as out:
            while True:
                chunk = await file.read(16384)
                if not chunk:
                    break
                out.write(chunk)
    finally:
        await file.close()

    web_path = f"/static/uploads/{name}"
    return {"path": web_path}



# ─────────────────────────────────────────────────────────────
# 系统
# ─────────────────────────────────────────────────────────────

@router.get("/health", tags=["system"], summary="健康检查")
async def health() -> dict:
    return {"status": "ok", "version": "1.0.0"}


# ─────────────────────────────────────────────────────────────
# 统计
# ─────────────────────────────────────────────────────────────

@router.get("/stats", tags=["stats"], summary="整体统计")
async def get_stats() -> dict:
    async with get_db() as db:
        total_tasks   = await db_count_tasks(db)
        pending       = await db_count_tasks(db, "pending")
        running       = await db_count_tasks(db, "running")
        done          = await db_count_tasks(db, "done")
        failed        = await db_count_tasks(db, "failed")
        cancelled     = await db_count_tasks(db, "cancelled")
        total_genomes = await db_count_genomes(db)
        # 收集所有唯一的 genome_dir
        async with db.execute("SELECT DISTINCT genome_dir FROM genomes WHERE genome_dir IS NOT NULL") as cur:
            dirs = [row[0] for row in await cur.fetchall()]

    # 按挂载点聚合磁盘用量（避免同一挂载点重复计算）
    seen_mounts: dict[str, dict] = {}
    dir_stats: list[dict] = []
    for d in dirs:
        p = Path(d)
        try:
            usage = shutil.disk_usage(d)
            mount = str(p.anchor)   # 粗略用根目录区分（Linux 单分区场景够用）
            # 更精确：找挂载点
            check = p
            while not check.is_mount():
                check = check.parent
            mount = str(check)
            seen_mounts[mount] = {
                "mount":  mount,
                "total":  usage.total,
                "used":   usage.used,
                "free":   usage.free,
                "percent": round(usage.used / usage.total * 100, 1) if usage.total else 0,
            }
        except OSError:
            pass
        dir_stats.append({"genome_dir": d})

    return {
        "tasks": {
            "total":     total_tasks,
            "pending":   pending,
            "running":   running,
            "done":      done,
            "failed":    failed,
            "cancelled": cancelled,
        },
        "genomes": {"total": total_genomes},
        "disk":    list(seen_mounts.values()),
    }


@router.get("/stats/disk", tags=["stats"], summary="指定目录磁盘用量")
async def get_disk_usage(path: str = Query(..., description="要查询的目录路径")) -> dict:
    """返回指定路径所在分区的磁盘使用情况。"""
    p = Path(path)
    if not p.exists():
        raise HTTPException(status_code=404, detail=f"路径不存在: {path}")
    try:
        usage = shutil.disk_usage(path)
    except OSError as e:
        raise HTTPException(status_code=500, detail=str(e))
    # 统计目录自身大小（仅含 non_ref/genomes_metadata.tsv 等轻量文件快速估算）
    return {
        "path":    path,
        "total":   usage.total,
        "used":    usage.used,
        "free":    usage.free,
        "percent": round(usage.used / usage.total * 100, 1) if usage.total else 0,
    }


@router.get("/stats/taxon", tags=["stats"], summary="各分类单元基因组统计")
async def get_taxon_stats() -> list:
    """以 taxon_configs 为数据源，返回每个 taxon 的基因组统计及待更新数。"""
    async with get_db() as db:
        async with db.execute("""
            SELECT
                t.taxon,
                COALESCE((
                    SELECT COUNT(*) FROM genomes g
                    WHERE g.taxon = t.taxon
                ), 0) AS total,
                COALESCE((
                    SELECT COUNT(*) FROM genomes g
                    WHERE g.taxon = t.taxon AND g.genome_type = 'ref'
                ), 0) AS ref_count,
                COALESCE((
                    SELECT SUM(pending_count) FROM taxon_configs tc
                    WHERE tc.taxon = t.taxon
                ), 0) AS pending_update,
                COALESCE((
                    SELECT SUM(pending_format_count) FROM taxon_configs tc
                    WHERE tc.taxon = t.taxon
                ), 0) AS pending_format,
                (
                    SELECT MAX(genome_date) FROM taxon_configs tc
                    WHERE tc.taxon = t.taxon
                ) AS genome_date,
                (
                    SELECT MAX(last_auto_updated) FROM taxon_configs tc
                    WHERE tc.taxon = t.taxon
                ) AS last_auto_updated,
                (
                    SELECT MIN(next_check_at) FROM taxon_configs tc
                    WHERE tc.taxon = t.taxon
                      AND tc.check_interval_days > 0
                ) AS next_check_at,
                (
                    SELECT MIN(next_download_at) FROM taxon_configs tc
                    WHERE tc.taxon = t.taxon
                      AND tc.download_interval_days > 0
                ) AS next_download_at,
                (
                    SELECT GROUP_CONCAT(DISTINCT tc.genome_dir) FROM taxon_configs tc
                    WHERE tc.taxon = t.taxon AND tc.genome_type = 'ref'
                ) AS ref_dirs,
                (
                    SELECT GROUP_CONCAT(DISTINCT tc.genome_dir) FROM taxon_configs tc
                    WHERE tc.taxon = t.taxon AND tc.genome_type != 'ref'
                ) AS other_dirs,
                (
                    SELECT tc.icon FROM taxon_configs tc
                    WHERE tc.taxon = t.taxon
                      AND tc.icon IS NOT NULL AND tc.icon != ''
                    LIMIT 1
                ) AS icon
            FROM (SELECT DISTINCT taxon FROM taxon_configs) t
            ORDER BY total DESC, t.taxon
        """) as cur:
            rows = await cur.fetchall()
    return [
        {
            "taxon":              r[0],
            "total":              r[1],
            "ref_count":          r[2],
            "pending_update":     r[3],
            "pending_format":     r[4],
            "genome_date":        r[5],
            "last_auto_updated":  r[6],
            "next_check_at":      r[7],
            "next_download_at":   r[8],
            "ref_dirs":           r[9].split(',') if r[9] else [],
            "other_dirs":         r[10].split(',') if r[10] else [],
            "icon":               r[11] or None,
        }
        for r in rows
    ]


# ─────────────────────────────────────────────────────────────
# Tasks
# ─────────────────────────────────────────────────────────────

@router.get("/tasks", tags=["tasks"], summary="任务列表")
async def list_tasks(
    status: Optional[Literal["pending", "running", "done", "failed", "cancelled"]] = None,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
) -> dict:
    async with get_db() as db:
        total = await db_count_tasks(db, status)
        items = await db_list_tasks(db, status, limit, offset)
    return {"total": total, "limit": limit, "offset": offset, "items": items}


@router.post("/tasks", status_code=201, tags=["tasks"], summary="提交新任务")
async def create_task(data: TaskCreate) -> dict:
    task_data = data.model_dump()
    # bool → int for SQLite
    task_data["overwrite"] = 1 if task_data.pop("overwrite") else 0
    async with get_db() as db:
        task_id = await db_create_task(db, task_data)
    await task_queue.submit(task_id)
    return {"id": task_id, "status": "pending"}


@router.get("/tasks/{task_id}", tags=["tasks"], summary="任务详情")
async def get_task(task_id: int) -> dict:
    async with get_db() as db:
        task = await db_get_task(db, task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")
    return task


@router.delete("/tasks/{task_id}", tags=["tasks"], summary="取消任务")
async def cancel_task(task_id: int) -> dict:
    async with get_db() as db:
        task = await db_get_task(db, task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")
    if task["status"] not in ("pending", "running"):
        raise HTTPException(
            status_code=400,
            detail=f"无法取消状态为 '{task['status']}' 的任务",
        )
    
    cancelled = await task_queue.cancel(task_id)
    
    if cancelled:
        async with get_db() as db:
            # 如果是排队中的任务，数据库状态已在 cancel 中更新；
            # 如果是运行中的任务，这里更新为 cancelled
            task = await db_get_task(db, task_id)
            if task["status"] == "running":
                finished = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                await db_update_task(db, task_id, status="cancelled", finished_at=finished, error_msg="用户取消")
                await db_add_log(db, task_id, "[INFO] 任务已被取消")
        return {"ok": True, "message": "任务已取消"}
    else:
        return {"ok": False, "message": "任务已结束或无法取消"}


@router.post("/tasks/batch-delete", tags=["tasks"], summary="批量删除任务及日志")
async def batch_delete_tasks(
    ids: list[int] = Body(..., embed=True),
) -> dict:
    """删除指定任务记录及其所有日志，运行中的任务会先取消再删除。"""
    if not ids:
        raise HTTPException(status_code=400, detail="ids 不能为空")
    # 先取消尚在运行/等待的任务
    for task_id in ids:
        async with get_db() as db:
            task = await db_get_task(db, task_id)
        if task and task["status"] in ("pending", "running"):
            await task_queue.cancel(task_id)
    async with get_db() as db:
        deleted = await db_delete_tasks(db, ids)
    return {"ok": True, "deleted": deleted}


# ─────────────────────────────────────────────────────────────
# Task Logs（非 SSE 的快照接口）
# ─────────────────────────────────────────────────────────────

@router.get("/tasks/{task_id}/logs", tags=["tasks"], summary="任务日志快照")
async def get_task_logs(task_id: int, after_id: int = 0) -> dict:
    async with get_db() as db:
        task = await db_get_task(db, task_id)
        if task is None:
            raise HTTPException(status_code=404, detail="Task not found")
        logs = await db_get_logs(db, task_id, after_id)
    return {"task_id": task_id, "status": task["status"], "logs": logs}


# ─────────────────────────────────────────────────────────────
# Taxon Configs
# ─────────────────────────────────────────────────────────────

@router.get("/configs", tags=["configs"], summary="配置列表")
async def list_configs() -> list:
    async with get_db() as db:
        configs = await db_list_taxon_configs(db)
    # 不返回 api_key 明文（留给 Phase 5 加密后再考虑）
    for c in configs:
        if c.get("api_key"):
            c["api_key"] = "********"
    return configs


@router.get('/configs/export', tags=['configs'], summary='导出所有配置（JSON）')
async def export_configs(include_secrets: bool = Query(False, description='是否包含 api_key 等敏感字段')) -> StreamingResponse:
    async with get_db() as db:
        configs = await db_list_taxon_configs(db)
    # 按需隐藏敏感字段
    out = []
    for c in configs:
        item = dict(c)
        if not include_secrets and item.get('api_key'):
            item['api_key'] = '********'
        out.append(item)

    def gen():
        yield (json.dumps(out, ensure_ascii=False, indent=2) + "\n").encode('utf-8')

    filename = 'taxon_configs_export.json'
    return StreamingResponse(gen(), media_type='application/json; charset=utf-8', headers={
        'Content-Disposition': f'attachment; filename="{filename}"'
    })


@router.post('/configs/import', tags=['configs'], summary='导入配置（JSON 列表）')
async def import_configs(payload: dict = Body(...)) -> dict:
    """导入配置。请求体可以是 {"configs": [...], "replace": bool} 或直接传入配置列表。
    导入时按 (taxon, genome_dir) 做合并（存在则更新，否则创建）。
    """
    # 支持直接传入列表或包裹在 configs 字段
    configs = None
    replace = False
    if isinstance(payload, list):
        configs = payload
    elif isinstance(payload, dict) and 'configs' in payload:
        configs = payload.get('configs')
        replace = bool(payload.get('replace', False))
    else:
        raise HTTPException(status_code=400, detail='请求体必须是配置列表或包含 configs 字段的对象')

    if not isinstance(configs, list):
        raise HTTPException(status_code=400, detail='configs 必须是一个数组')

    created = 0
    updated = 0
    async with get_db() as db:
        if replace:
            await db.execute('DELETE FROM taxon_configs')
            await db.commit()
        for raw in configs:
            if not isinstance(raw, dict):
                continue
            # 必要字段检查
            name = raw.get('name')
            taxon = raw.get('taxon')
            genome_dir = raw.get('genome_dir')
            if not name or not taxon or not genome_dir:
                continue
            # 规范化字段并计算 next_*
            data = dict(raw)
            data['do_check'] = data.get('do_download', data.get('do_check', True))
            now = datetime.now()
            for step in ('predownload', 'download', 'process'):
                days = int(data.get(f"{step}_interval_days", 0) or 0)
                data[f"next_{step}_at"] = (
                    (now + timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S") if days > 0 else None
                )

            # 查找已存在配置（以 taxon+genome_dir 为准）
            async with db.execute(
                "SELECT id FROM taxon_configs WHERE taxon=? AND genome_dir=?",
                (taxon, genome_dir),
            ) as cur:
                row = await cur.fetchone()
            if row:
                cfg_id = row[0]
                await db_update_taxon_config(db, cfg_id, data)
                updated += 1
            else:
                cfg_id = await db_create_taxon_config(db, data)
                created += 1
            # 同步写入 .ngm/config.json
            cfg = await db_get_taxon_config(db, cfg_id)
            _init_genome_repo(cfg)

    return {"created": created, "updated": updated}


@router.post("/configs", status_code=201, tags=["configs"], summary="新建配置")
async def create_config(data: TaxonConfigCreate) -> dict:
    payload = data.model_dump()
    # check 与 download 已合并，确保 do_check 全程与 do_download 保持同步
    payload["do_check"] = payload.get("do_download", 1)
    # 计算各步骤首次执行时间
    now = datetime.now()
    for step in ("predownload", "download", "process"):
        days = payload.get(f"{step}_interval_days", 0)
        payload[f"next_{step}_at"] = (
            (now + timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S") if days > 0 else None
        )
    async with get_db() as db:
        config_id = await db_create_taxon_config(db, payload)
        config = await db_get_taxon_config(db, config_id)
    _init_genome_repo(config)  # 初始化 genome_dir/.ngm/
    return config  # type: ignore[return-value]


@router.get("/configs/{config_id}", tags=["configs"], summary="配置详情")
async def get_config(config_id: int) -> dict:
    async with get_db() as db:
        config = await db_get_taxon_config(db, config_id)
    if config is None:
        raise HTTPException(status_code=404, detail="Config not found")
    if config.get("api_key"):
        config["api_key"] = "********"
    return config


@router.put("/configs/{config_id}", tags=["configs"], summary="更新配置")
async def update_config(config_id: int, data: TaxonConfigCreate) -> dict:
    async with get_db() as db:
        existing = await db_get_taxon_config(db, config_id)
        if existing is None:
            raise HTTPException(status_code=404, detail="Config not found")
        update_data = data.model_dump()
        # check 与 download 已合并，确保 do_check 全程与 do_download 保持同步
        update_data["do_check"] = update_data.get("do_download", 1)
        # 若 api_key 为空字符串，保留原値
        if not update_data.get("api_key"):
            update_data.pop("api_key", None)
        # 重新计算各步骤下次执行时间
        now = datetime.now()
        for step in ("predownload", "download", "process"):
            days = update_data.get(f"{step}_interval_days", 0)
            update_data[f"next_{step}_at"] = (
                (now + timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S") if days > 0 else None
            )
        await db_update_taxon_config(db, config_id, update_data)
        config = await db_get_taxon_config(db, config_id)
    _init_genome_repo(config)  # 同步更新 .ngm/config.json
    return config  # type: ignore[return-value]


@router.delete("/configs/{config_id}", tags=["configs"], summary="删除配置")
async def delete_config(config_id: int) -> dict:
    async with get_db() as db:
        existing = await db_get_taxon_config(db, config_id)
        if existing is None:
            raise HTTPException(status_code=404, detail="Config not found")
        await db_delete_taxon_config(db, config_id)
        # 同步删除该 taxon+genome_dir 下的基因组记录
        await db.execute(
            "DELETE FROM genomes WHERE taxon=? AND genome_dir=?",
            (existing["taxon"], existing["genome_dir"]),
        )
        await db.commit()
    return {"ok": True}


@router.post("/configs/{config_id}/trigger", tags=["configs"], summary="立即触发更新任务")
async def trigger_config_update(config_id: int) -> dict:
    """不管更新周期，立即为指定配置提交一个下载任务。"""
    async with get_db() as db:
        cfg = await db_get_taxon_config(db, config_id)
    if cfg is None:
        raise HTTPException(status_code=404, detail="Config not found")
    task_data = {
        "taxon":              cfg["taxon"],
        "genome_type":        cfg["genome_type"],
        "genome_dir":         cfg["genome_dir"],
        "tmp_dir":            cfg.get("tmp_dir"),
        "api_key":            cfg.get("api_key"),
        "threads":            cfg["threads"],
        "batch_size":         cfg["batch_size"],
        "parallel_downloads": cfg["parallel_downloads"],
        "overwrite":          1 if cfg.get("overwrite") else 0,
        "do_check":           cfg.get("do_check",    1),
        "do_download":        cfg.get("do_download", 1),
        "do_process":         cfg.get("do_process",  1),
    }
    async with get_db() as db:
        task_id = await db_create_task(db, task_data)
    await task_queue.submit(task_id)
    return {"task_id": task_id, "status": "pending"}


@router.post("/configs/{config_id}/run_step", tags=["configs"], summary="单步执行（预下载/合并/格式化）")
async def run_config_step(
    config_id: int,
    step: Literal["predownload", "merge", "process"],
) -> dict:
    """执行指定的单一步骤（忽略配置中的步骤开关）。
    - predownload：检查+下载到暂存区（check 与 predownload 已合并），不影响 live 目录
    - merge      ：合并更新＝从预下载目录移动文件到 live，更新 md5 和元数据（不下载序列）
    - process    ：格式化基因组＝仅处理已暂存文件（跳过下载）
    """
    async with get_db() as db:
        cfg = await db_get_taxon_config(db, config_id)
    if cfg is None:
        raise HTTPException(status_code=404, detail="Config not found")
    task_data = {
        "taxon":              cfg["taxon"],
        "genome_type":        cfg["genome_type"],
        "genome_dir":         cfg["genome_dir"],
        "tmp_dir":            cfg.get("tmp_dir"),
        "api_key":            cfg.get("api_key"),
        "threads":            cfg["threads"],
        "batch_size":         cfg["batch_size"],
        "parallel_downloads": cfg["parallel_downloads"],
        "overwrite":          1 if cfg.get("overwrite") else 0,
        "do_check":    1 if step == "predownload" else 0,
        "do_download": 1 if step in ("predownload", "merge") else 0,
        "do_process":  1 if step == "process" else 0,
    }
    async with get_db() as db:
        task_id = await db_create_task(db, task_data)
    await task_queue.submit(task_id)
    return {"task_id": task_id, "status": "pending", "step": step}


# ─────────────────────────────────────────────────────────────
# Genomes
# ─────────────────────────────────────────────────────────────

@router.get("/genomes", tags=["genomes"], summary="基因组列表")
async def list_genomes(
    search: Optional[str] = None,
    genome_type: Optional[Literal["ref", "all"]] = None,
    genome_dir: Optional[str] = None,
    taxon: Optional[str] = None,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
) -> dict:
    async with get_db() as db:
        total = await db_count_genomes(db, search, genome_type, genome_dir, taxon)
        items = await db_list_genomes(db, search, genome_type, genome_dir, limit, offset, taxon)
    return {"total": total, "limit": limit, "offset": offset, "items": items}


@router.get("/genomes/export", tags=["genomes"], summary="导出基因组列表（TSV）")
async def export_genomes(
    search: Optional[str] = None,
    genome_type: Optional[Literal["ref", "all"]] = None,
    genome_dir: Optional[str] = None,
    taxon: Optional[str] = None,
) -> StreamingResponse:
    """返回当前筛选结果的 TSV 文件（最多 50000 条）。"""
    async with get_db() as db:
        items = await db_list_genomes(db, search, genome_type, genome_dir, limit=50000, offset=0, taxon=taxon)

    def _generate():
        yield "GCA\t物种名称\t序列长度(bp)\tTaxID\t谱系\t类型\t更新时间\n"
        for g in items:
            lineage = (g.get("lineage") or "").replace("\t", " ")
            yield (
                f"{g.get('gca','')}\t"
                f"{(g.get('organism') or '').replace('_',' ')}\t"
                f"{g.get('length','') or ''}\t"
                f"{g.get('taxid','') or ''}\t"
                f"{lineage}\t"
                f"{g.get('genome_type','') or ''}\t"
                f"{g.get('last_updated','') or ''}\n"
            )

    filename = "genomes_export.tsv"
    return StreamingResponse(
        _generate(),
        media_type="text/tab-separated-values; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/genomes/groups", tags=["genomes"], summary="基因组分组概览（按 taxon 分组）")
async def genome_groups() -> list:
    async with get_db() as db:
        return await db_get_genome_groups(db)


@router.post("/genomes/sync", tags=["genomes"], summary="从 non_ref/genomes_metadata.tsv 同步基因组到数据库")
async def sync_genomes(
    genome_dir: str = Query(..., description="基因组存储目录"),
    taxon: Optional[str] = Query(None, description="分类单元名（可选）"),
) -> dict:
    tsv = non_ref_metadata_path(Path(genome_dir))
    if not tsv.exists():
        raise HTTPException(status_code=404, detail=f"未找到 {tsv}")
    from ..tasks import _sync_genomes_from_tsv
    synced = await _sync_genomes_from_tsv(genome_dir, taxon)
    return {"synced": synced, "genome_dir": genome_dir}


# ─────────────────────────────────────────────────────────────
# 完整性校验
# ─────────────────────────────────────────────────────────────

@router.post("/integrity_check", tags=["integrity"], summary="启动基因组完整性校验（MD5）")
async def start_integrity_check(
    genome_dir: str = Body(..., embed=True, description="基因组存储目录"),
    config_id: Optional[int] = Body(None, embed=True, description="关联配置 ID（修复时使用）"),
    force_new: bool = Body(False, embed=True, description="是否强制重新校验（默认复用最近一次）"),
    workers: Optional[int] = Body(None, embed=True, description="并行校验线程数（默认使用配置 threads）"),
) -> dict:
    """启动对指定目录下所有基因组文件的 MD5 校验任务，立即返回 check_id。"""
    from ..tasks import start_integrity_check_task

    resolved_workers = workers
    if resolved_workers is None and config_id is not None:
        async with get_db() as db:
            cfg = await db_get_taxon_config(db, config_id)
        if cfg and cfg.get("threads"):
            resolved_workers = int(cfg["threads"])
    if resolved_workers is None:
        resolved_workers = 4

    check_id, reused = start_integrity_check_task(
        genome_dir,
        config_id,
        reuse_running=True,
        force_new=force_new,
        workers=resolved_workers,
    )
    return {"check_id": check_id, "reused": reused}


@router.get("/integrity_check/{check_id}", tags=["integrity"], summary="获取校验进度与报告")
async def get_integrity_check(check_id: str) -> dict:
    """轮询获取校验任务的当前进度和最终报告。"""
    from ..tasks import _integrity_checks
    state = _integrity_checks.get(check_id)
    if state is None:
        raise HTTPException(status_code=404, detail="校验任务不存在或已过期")
    return state


@router.post("/integrity_check/{check_id}/cancel", tags=["integrity"], summary="取消完整性校验")
async def cancel_integrity_check(check_id: str) -> dict:
    """取消正在运行中的完整性校验任务。"""
    from ..tasks import cancel_integrity_check_task
    ok = cancel_integrity_check_task(check_id)
    if not ok:
        raise HTTPException(status_code=400, detail="校验任务不存在或不在运行中")
    return {"ok": True, "check_id": check_id}


@router.post("/integrity_check/{check_id}/repair", tags=["integrity"], summary="修复校验失败的基因组文件")
async def repair_integrity(check_id: str, config_id: int = Body(..., embed=True)) -> dict:
    """删除损坏文件并清除其 md5 记录，然后提交预下载任务重新获取。"""
    from ..tasks import _integrity_checks, task_queue

    state = _integrity_checks.get(check_id)
    if state is None:
        raise HTTPException(status_code=404, detail="校验任务不存在或已过期")
    if state["status"] not in ("done", "cancelled"):
        raise HTTPException(status_code=400, detail="校验尚未完成，请等待校验结束后再修复")

    failed_gcas: list[str] = [f["gca"] for f in state.get("failed_files", [])]
    missing_gcas: list[str] = list(state.get("missing_files", []))
    to_fix = set(failed_gcas + missing_gcas)

    if not to_fix:
        raise HTTPException(status_code=400, detail="没有需要修复的文件")

    genome_dir = Path(state["genome_dir"])

    # 1. 删除损坏的 .fna 文件
    deleted = 0
    for f in state.get("failed_files", []):
        fna_path = Path(f["path"])
        if fna_path.exists():
            try:
                fna_path.unlink()
                deleted += 1
            except OSError:
                pass

    # 2. 从两个 md5sums 文件中删除这些 GCA 的条目，使下载计划器认为它们尚未下载
    for md5_file in (non_ref_md5_path(genome_dir), ref_md5_path(genome_dir)):
        if not md5_file.exists():
            continue
        lines = md5_file.read_text().splitlines()
        new_lines = [ln for ln in lines if not any(ln.startswith(gca) for gca in to_fix)]
        md5_file.write_text("\n".join(new_lines) + ("\n" if new_lines else ""))

    # 3. 获取配置并提交预下载任务（check + download，不处理）
    async with get_db() as db:
        cfg = await db_get_taxon_config(db, config_id)
    if cfg is None:
        raise HTTPException(status_code=404, detail="配置不存在")

    task_data = {
        "taxon":              cfg["taxon"],
        "genome_type":        cfg["genome_type"],
        "genome_dir":         cfg["genome_dir"],
        "tmp_dir":            cfg.get("tmp_dir"),
        "api_key":            cfg.get("api_key"),
        "threads":            cfg["threads"],
        "batch_size":         cfg["batch_size"],
        "parallel_downloads": cfg["parallel_downloads"],
        "overwrite":          1,   # 强制重新拉取元数据，确保这些 GCA 被识别为待下载
        "do_check":    1,
        "do_download": 1,
        "do_process":  0,
    }
    async with get_db() as db:
        task_id = await db_create_task(db, task_data)
    await task_queue.submit(task_id)

    return {"task_id": task_id, "repaired": len(to_fix), "deleted": deleted}


# ─────────────────────────────────────────────────────────────
# BLAST 数据库完整性校验
# ─────────────────────────────────────────────────────────────

@router.post("/blast_db_check", tags=["blast_db"], summary="启动 BLAST 数据库完整性校验")
async def start_blast_db_check(
    genome_dir: str = Body(..., embed=True, description="基因组存储目录"),
    config_id: Optional[int] = Body(None, embed=True, description="关联配置 ID"),
    force_new: bool = Body(False, embed=True, description="是否强制重新校验"),
    workers: Optional[int] = Body(None, embed=True, description="并行线程数"),
) -> dict:
    """启动对指定目录下所有已建库 BLAST 数据库的完整性校验，立即返回 check_id。"""
    from ..tasks import start_blast_db_check_task

    resolved_workers = workers
    if resolved_workers is None and config_id is not None:
        async with get_db() as db:
            from ..database import db_get_taxon_config
            cfg = await db_get_taxon_config(db, config_id)
        if cfg and cfg.get("threads"):
            resolved_workers = int(cfg["threads"])
    if resolved_workers is None:
        resolved_workers = 4

    check_id, reused = start_blast_db_check_task(
        genome_dir,
        config_id,
        reuse_running=True,
        force_new=force_new,
        workers=resolved_workers,
    )
    return {"check_id": check_id, "reused": reused}


@router.get("/blast_db_check/{check_id}", tags=["blast_db"], summary="获取 BLAST 数据库校验进度")
async def get_blast_db_check(check_id: str) -> dict:
    """轮询获取 BLAST 数据库校验任务的当前进度和最终报告。"""
    from ..tasks import _blast_db_checks
    state = _blast_db_checks.get(check_id)
    if state is None:
        raise HTTPException(status_code=404, detail="校验任务不存在或已过期")
    return state


@router.post("/blast_db_check/{check_id}/cancel", tags=["blast_db"], summary="取消 BLAST 数据库校验")
async def cancel_blast_db_check(check_id: str) -> dict:
    """取消正在运行中的 BLAST 数据库校验任务。"""
    from ..tasks import cancel_blast_db_check_task
    ok = cancel_blast_db_check_task(check_id)
    if not ok:
        raise HTTPException(status_code=400, detail="校验任务不存在或不在运行中")
    return {"ok": True, "check_id": check_id}


# ─────────────────────────────────────────────────────────────
# FAI 索引构建
# ─────────────────────────────────────────────────────────────

@router.post("/fai_index", tags=["fai_index"], summary="启动 FAI 索引构建任务")
async def start_fai_index(
    genome_dir: str = Body(..., embed=True, description="基因组存储目录"),
    workers: Optional[int] = Body(None, embed=True, description="并行线程数"),
) -> dict:
    """为指定目录下所有缺少 .fai 的 .fna 文件启动 samtools faidx 索引构建，立即返回 task_id。"""
    from ..tasks import start_fai_index_task

    resolved_workers = max(1, workers) if workers else 4
    task_id, reused = start_fai_index_task(genome_dir, workers=resolved_workers)
    return {"task_id": task_id, "reused": reused}


@router.get("/fai_index/{task_id}", tags=["fai_index"], summary="获取 FAI 索引构建进度")
async def get_fai_index(task_id: str) -> dict:
    """轮询获取 FAI 索引构建任务的当前进度和最终报告。"""
    from ..tasks import _fai_tasks_state
    state = _fai_tasks_state.get(task_id)
    if state is None:
        raise HTTPException(status_code=404, detail="任务不存在或已过期")
    return state


@router.post("/fai_index/{task_id}/cancel", tags=["fai_index"], summary="取消 FAI 索引构建")
async def cancel_fai_index(task_id: str) -> dict:
    """取消正在运行中的 FAI 索引构建任务。"""
    from ..tasks import cancel_fai_index_task
    ok = cancel_fai_index_task(task_id)
    if not ok:
        raise HTTPException(status_code=400, detail="任务不存在或不在运行中")
    return {"ok": True, "task_id": task_id}
