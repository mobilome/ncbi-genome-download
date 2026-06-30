#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
任务队列模块 — 任务执行、日志捕获、状态管理、自动更新调度。

组件：
- TaskQueue  : 按分类单元并发调度，相同 taxon 串行，不同 taxon 并发
- AutoUpdateScheduler : 每小时检查到期的 taxon_configs，自动提交任务
"""
from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
import hashlib
import logging
import os
import re
import signal
import sys
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from genome_downloader.utils import (
    non_ref_blastdb_dir,
    non_ref_genomes_dir,
    non_ref_md5_path,
    non_ref_metadata_path,
    ref_blastdb_dir,
    ref_genomes_dir,
    ref_md5_path,
    ref_metadata_path,
)

from .database import (
    db_add_log,
    db_create_task,
    db_get_steps_due_for_update,
    db_get_task,
    db_update_task,
    db_update_taxon_config,
    db_upsert_genome,
    get_db,
)

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# 日志清洗工具
# ─────────────────────────────────────────────────────────────

# 匹配 ANSI/终端转义序列（颜色代码、光标移动 \x1b[1A、清行 \x1b[2K 等）
_ANSI_RE = re.compile(r'\x1b\[[0-9;]*[A-Za-z]|\x1b[A-Za-z]')

# 匹配 datasets CLI 进度条行的分组前缀（用于去重压缩）
_PROGRESS_GROUP_RE = re.compile(r'^(Collecting|Completed|Downloading: |Validating package|Found \d+)')
_PROGRESS_PERCENT_RE = re.compile(r'(\d{1,3})%')


def _clean_line(text: str) -> str:
    """剥离 ANSI/终端转义序列，返回纯文本。"""
    cleaned = _ANSI_RE.sub('', text)
    if '\r' in cleaned:
        cleaned = cleaned.split('\r')[-1]
    return cleaned

# 全局进程表：task_id -> asyncio.subprocess.Process（用于 cancel）
_processes: dict[int, asyncio.subprocess.Process] = {}


# ─────────────────────────────────────────────────────────────
# 内部工具
# ─────────────────────────────────────────────────────────────

def _build_cmd(task: dict) -> list[str]:
    """根据 task 参数构建子进程命令列表。"""
    cmd = [
        sys.executable, "-m", "genome_downloader",
        "--taxon",      task["taxon"],
        "--genome_dir", task["genome_dir"].strip(),
        "--genome_type", task["genome_type"],
        "--threads",    str(task["threads"]),
        "--batch_size", str(task["batch_size"]),
        "--parallel_downloads", str(task["parallel_downloads"]),
    ]
    if task.get("overwrite"):
        cmd.append("--overwrite")
    if task.get("tmp_dir"):
        cmd += ["--tmp_dir", task["tmp_dir"].strip()]
    if task.get("api_key"):
        cmd += ["--api_key", task["api_key"]]
    # 步骤开关：check 与 download 已合并，do_check=True 时 do_download 自动为 True
    if not task.get("do_check",    True):
        cmd.append("--skip_check")
    # --skip_download 仅用于「格式化基因组」步骤（从暂存区处理，跳过下载）
    if not task.get("do_download", True) and not task.get("do_check", True):
        cmd.append("--skip_download")
    if not task.get("do_process",  True):
        cmd.append("--skip_process")
    if task.get("do_validate_db", False):
        cmd.append("--validate_db")
    return cmd


async def _sync_genomes_from_tsv(
    genome_dir: str,
    taxon: str | None = None,
    tsv_path: Path | None = None,
) -> int:
    """扫描指定 metadata TSV，将条目同步到 genomes 表。

    Args:
        genome_dir: 基因组存储根目录。
        taxon:      对应的分类单元名（可选，写入 genomes.taxon 列）。
        tsv_path:   TSV 文件路径，默认 non_ref/genomes_metadata.tsv。

    Returns:
        同步的条目数量。
    """
    genome_root = Path(genome_dir)
    tsv = tsv_path or non_ref_metadata_path(genome_root)
    if not tsv.exists():
        return 0

    rows: list[dict] = []
    with tsv.open("r") as f:
        header = f.readline().strip().split("\t")
        col = {name: idx for idx, name in enumerate(header)}
        for line in f:
            parts = line.strip().split("\t")
            if not parts or not parts[0]:
                continue
            rows.append({
                "gca":         parts[col.get("GCA", 0)],
                "organism":    parts[col.get("Organism", 1)]  if len(parts) > col.get("Organism", 1)  else None,
                "length":      int(parts[col.get("Length",  2)]) if len(parts) > col.get("Length",  2) and parts[col.get("Length", 2)].isdigit() else None,
                "taxid":       parts[col.get("TaxID",   3)]  if len(parts) > col.get("TaxID",   3)  else None,
                "lineage":     parts[col.get("Lineage", 4)]  if len(parts) > col.get("Lineage", 4)  else None,
                "genome_type": parts[col.get("Type",    5)]  if len(parts) > col.get("Type",    5)  else None,
                "genome_dir":  genome_dir,
                "taxon":       taxon,
            })

    if not rows:
        return 0

    async with get_db() as db:
        for row in rows:
            await db_upsert_genome(db, row)
        await db.commit()

    return len(rows)


async def _sync_genomes_from_download(task: dict) -> int:
    """下载完成后，从最终存储目录扫描已验证的基因组并同步最小记录到 genomes 表。

    download_genomes() 将通过 MD5 校验的文件移入 genome_dir/non_ref/genomes/<GCA>.fna，
    rebuild_ref_links() 再将 ref 类型移入 genome_dir/ref/genomes/<GCA>.fna。
    此函数扫描两个目录，为每个已下载的基因组插入/更新最小记录。
    process 完成后 _sync_genomes_from_tsv 会用完整元数据覆盖这些记录。
    """
    GCA_RE     = re.compile(r"^GCA_\d+\.\d+$")
    genome_dir = Path(task["genome_dir"])

    rows: list[dict] = []
    for scan_dir in (non_ref_genomes_dir(genome_dir), ref_genomes_dir(genome_dir)):
        if not scan_dir.exists():
            continue
        for fna_file in scan_dir.iterdir():
            if not fna_file.is_file() or fna_file.suffix != ".fna":
                continue
            gca = fna_file.stem
            if not GCA_RE.match(gca):
                continue
            rows.append({
                "gca":         gca,
                "organism":    None,
                "length":      None,
                "taxid":       None,
                "lineage":     None,
                "genome_type": task.get("genome_type", "ref"),
                "genome_dir":  task["genome_dir"],
                "taxon":       task["taxon"],
            })

    if not rows:
        return 0

    async with get_db() as db:
        for row in rows:
            await db_upsert_genome(db, row)
        await db.commit()

    return len(rows)

async def _count_pending_genomes(task: dict) -> int | None:
    """读取 download_list.txt，返回待下载的基因组数量。"""
    tmp = task.get("tmp_dir")
    genome_dir = task["genome_dir"]
    candidates = []
    if tmp:
        candidates.append(Path(tmp) / "download_list.txt")
    # genome_downloader 默认使用 genome_dir/tmp 作为工作目录
    candidates.append(Path(genome_dir) / "tmp" / "download_list.txt")
    # 兼容旧命名（含 taxon 子目录）
    if tmp:
        candidates += [
            Path(tmp) / task["taxon"] / "download_list.txt",
            Path(tmp) / task["taxon"] / f"{task['taxon']}.clean.genome_to_download",
        ]
    for pending_file in candidates:
        if pending_file.exists():
            try:
                with pending_file.open() as f:
                    return sum(1 for line in f if line.strip())
            except OSError:
                return None


async def _count_staged_genomes(task: dict) -> int:
    """计算预下载暂存区（predownload/）中已暂存的基因组 .fna 文件数量。"""
    tmp = task.get("tmp_dir")
    if tmp:
        predownload_dir = Path(tmp) / "predownload"
    else:
        # genome_downloader 默认将工作目录放在 genome_dir/tmp
        predownload_dir = Path(task["genome_dir"]) / "tmp" / "predownload"
    if not predownload_dir.exists():
        return 0
    return sum(1 for f in predownload_dir.iterdir() if f.is_file() and f.suffix == ".fna")

# ─────────────────────────────────────────────────────────────
# TaskQueue
# ─────────────────────────────────────────────────────────────

class TaskQueue:
    """任务调度器：不同分类单元并发执行，相同分类单元串行执行。

    - 每个 (taxon, genome_dir) 组合对应一把 asyncio.Lock。
    - dispatcher 协程持续从队列取出任务并立即启动独立 asyncio.Task。
    - 同一 taxon 的任务竞争同一把锁，保证串行；不同 taxon 无锁竞争，天然并发。
    """

    def __init__(self) -> None:
        self._queue: asyncio.Queue[int] = asyncio.Queue()
        self._pending: set[int] = set()          # 已提交但 dispatcher 尚未取走的 task_id
        self._running: dict[int, asyncio.Task] = {}   # 已分发的 task_id → asyncio.Task
        self._taxon_locks: dict[str, asyncio.Lock] = {}  # taxon_key → Lock
        self._dispatcher_task: Optional[asyncio.Task] = None  # type: ignore[type-arg]

    def _taxon_key(self, task: dict) -> str:
        """同一 (taxon, genome_dir) 共享一把互斥锁。"""
        return f"{task['taxon']}::{task['genome_dir']}"

    def _get_taxon_lock(self, key: str) -> asyncio.Lock:
        if key not in self._taxon_locks:
            self._taxon_locks[key] = asyncio.Lock()
        return self._taxon_locks[key]

    async def start(self) -> None:
        self._dispatcher_task = asyncio.create_task(
            self._dispatcher(), name="genome_task_dispatcher"
        )
        logger.info("TaskQueue dispatcher started (per-taxon concurrent mode).")

    async def _dispatcher(self) -> None:
        """从队列取出 task_id，为每个任务启动独立 asyncio.Task 后立即返回等待下一个。"""
        while True:
            task_id = await self._queue.get()
            self._pending.discard(task_id)
            t = asyncio.create_task(
                self._run_task_with_lock(task_id),
                name=f"genome_task_{task_id}",
            )
            self._running[task_id] = t
            t.add_done_callback(lambda _fut, tid=task_id: self._running.pop(tid, None))
            self._queue.task_done()

    async def _run_task_with_lock(self, task_id: int) -> None:
        """获取分类单元锁后执行任务，保证同 taxon 任务串行。"""
        async with get_db() as db:
            task = await db_get_task(db, task_id)
        if task is None:
            logger.warning("Task %d not found in DB, skipping.", task_id)
            return
        if task["status"] != "pending":
            logger.info("Task %d already in state '%s', skipping.", task_id, task["status"])
            return

        key = self._taxon_key(task)
        lock = self._get_taxon_lock(key)

        async with lock:
            # 等待锁期间状态可能已变更（被取消或重复提交），再次检查
            async with get_db() as db:
                task = await db_get_task(db, task_id)
            if task is None or task["status"] != "pending":
                logger.info("Task %d: no longer pending after acquiring lock (status=%s), skipping.",
                            task_id, task["status"] if task else "None")
                return

            try:
                await self._run_task(task_id)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("Unhandled error in task %d: %s", task_id, exc)

    async def _run_task(self, task_id: int) -> None:
        async with get_db() as db:
            task = await db_get_task(db, task_id)

        if task is None:
            logger.warning("Task %d not found in DB, skipping.", task_id)
            return

        if task["status"] != "pending":
            logger.info("Task %d already in state '%s', skipping duplicate execution.", task_id, task["status"])
            return

        cmd = _build_cmd(task)
        started = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        async with get_db() as db:
            await db_update_task(db, task_id, status="running", started_at=started)
            await db_add_log(db, task_id, f"[RUN] 启动子进程: {' '.join(cmd)}")

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            limit=2 ** 20,  # 1 MB，防止无换行的进度条输出填满默认 64 KB 缓冲区导致 LimitOverrunError
            preexec_fn=os.setsid,  # 创建新进程组，便于取消时杀死所有子进程
        )
        _processes[task_id] = proc

        async with get_db() as db:
            await db_update_task(db, task_id, pid=proc.pid)

        try:
            assert proc.stdout is not None
            # 进度条分组追踪：保留每组最新一行，按阈值/间隔输出
            _pg_key: str | None = None    # 当前进度条组的前缀 key
            _pg_line: str | None = None   # 该组最新一行（待写入）
            _pg_logged: set[str] = set()  # 已写入 DB 的进度条行（去重）
            _pg_last_emit_at: dict[str, float] = {}
            _pg_last_percent: dict[str, int] = {}
            _pg_emit_interval = 10.0  # 秒：无百分比进度时的最小输出间隔
            _pg_emit_step = 5         # 百分比进度的最小增量

            async for raw_line in proc.stdout:
                raw_text = _clean_line(raw_line.decode("utf-8", errors="replace"))
                # datasets 进度常用 \r 覆盖同一行；此处按逻辑行拆分并压缩
                for line in raw_text.splitlines():
                    line = line.strip()
                    if not line:
                        continue

                    m = _PROGRESS_GROUP_RE.match(line)
                    if m:
                        key = m.group(1)
                        # 关键状态应立即写入，非关键状态按阈值/间隔输出
                        is_final = any(s in line for s in ['done', 'valid', '100%'])
                        
                        if key != _pg_key:
                            # 分组切换：将上一组最后一行写入 DB
                            if _pg_line is not None and _pg_line not in _pg_logged:
                                async with get_db() as db:
                                    await db_add_log(db, task_id, _pg_line)
                                _pg_logged.add(_pg_line)
                            _pg_key = key

                        _pg_line = line  # 始终保留最新进度

                        now = time.monotonic()
                        percent_match = _PROGRESS_PERCENT_RE.search(line)
                        should_emit = False
                        if percent_match:
                            percent = int(percent_match.group(1))
                            last_percent = _pg_last_percent.get(key, -1)
                            if percent >= 100:
                                is_final = True
                            if is_final or percent >= last_percent + _pg_emit_step:
                                _pg_last_percent[key] = percent
                                should_emit = True
                        else:
                            last_emit = _pg_last_emit_at.get(key, 0.0)
                            if is_final or (now - last_emit) >= _pg_emit_interval:
                                should_emit = True

                        if should_emit and _pg_line not in _pg_logged:
                            async with get_db() as db:
                                await db_add_log(db, task_id, _pg_line)
                            _pg_logged.add(_pg_line)
                            _pg_last_emit_at[key] = now
                    else:
                        # 普通日志行：先刷出待写的进度条行
                        if _pg_line is not None and _pg_line not in _pg_logged:
                            async with get_db() as db:
                                await db_add_log(db, task_id, _pg_line)
                            _pg_logged.add(_pg_line)
                        _pg_key = None
                        _pg_line = None
                        async with get_db() as db:
                            await db_add_log(db, task_id, line)

            # 循环结束后刷出最后一个进度条行
            if _pg_line is not None and _pg_line not in _pg_logged:
                async with get_db() as db:
                    await db_add_log(db, task_id, _pg_line)
        except asyncio.CancelledError:
            try:
                proc.terminate()
            except ProcessLookupError:
                pass
            raise
        except (ValueError, OSError) as exc:
            # ValueError: asyncio StreamReader LimitOverrunError（单行超过缓冲区限制，通常来自无换行的进度输出）
            # OSError: 管道提前关闭等 I/O 异常
            # 不中断任务，等待子进程退出后由 returncode 决定最终状态
            logger.warning("Task %d: log-read error (ignored): %s", task_id, exc)
        finally:
            _processes.pop(task_id, None)

        returncode = await proc.wait()
        finished = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if returncode == 0:
            # 同步基因组元数据（异常不阻断状态更新）
            synced = 0
            try:
                if task.get("do_process"):
                    # 同时同步两个元数据文件：non_ref + ref
                    genome_root = Path(task["genome_dir"])
                    synced  = await _sync_genomes_from_tsv(task["genome_dir"], task.get("taxon"))
                    synced += await _sync_genomes_from_tsv(
                        task["genome_dir"], task.get("taxon"),
                        tsv_path=ref_metadata_path(genome_root),
                    )
                elif task.get("do_download"):
                    # 今 non_ref/genomes_metadata.tsv 同步（含正确的 genome_type per GCA）
                    genome_root = Path(task["genome_dir"])
                    _tsv = non_ref_metadata_path(genome_root)
                    if _tsv.exists():
                        synced  = await _sync_genomes_from_tsv(task["genome_dir"], task.get("taxon"))
                        synced += await _sync_genomes_from_tsv(
                            task["genome_dir"], task.get("taxon"),
                            tsv_path=ref_metadata_path(genome_root),
                        )
                    else:
                        synced = await _sync_genomes_from_download(task)
            except Exception as exc:
                logger.warning("Task %d: post-completion genome sync failed: %s", task_id, exc)

            # 更新待更新/待格式化/预下载计数（异常不阻断状态更新）
            pending_count: int | None = None
            pending_format_count: int | None = None
            predownload_count: int | None = None
            try:
                # check 与 download 已合并，直接处理 download 完成后的计数
                if task.get("do_download"):
                    if task.get("do_process"):
                        # 完整流程：下载+处理，全部清零
                        pending_count = 0
                        pending_format_count = 0
                        predownload_count = 0
                    elif task.get("do_check"):
                        # 预下载模式（check+download, no process）：基因组已暂存，等待格式化
                        staged = await _count_staged_genomes(task)
                        pending_count = staged        # 暂存中尚未格式化的基因组数
                        predownload_count = staged
                        pending_format_count = staged
                    else:
                        # 合并更新模式（merge）：文件已从 predownload/ 移动到 genomes/
                        pending_count = 0
                        predownload_count = await _count_staged_genomes(task)  # 剩余未合并
                        pending_format_count = 0
                if task.get("do_process") and not task.get("do_download"):
                    # process-only（应用预下载）：清零预下载和待格式化计数
                    pending_count = 0
                    pending_format_count = 0
                    predownload_count = 0
            except Exception as exc:
                logger.warning("Task %d: post-completion count update failed: %s", task_id, exc)

            async with get_db() as db:
                if synced:
                    await db_add_log(db, task_id, f"[INFO] 已同步 {synced} 条基因组记录到数据库")
                # 始终更新 last_auto_updated（含手动触发的任务）
                db_updates: dict[str, object] = {"last_auto_updated": finished}
                if pending_count is not None:
                    db_updates["pending_count"] = pending_count
                    if pending_count > 0:
                        await db_add_log(db, task_id, f"[INFO] 检查到 {pending_count} 个待更新基因组")
                if pending_format_count is not None:
                    db_updates["pending_format_count"] = pending_format_count
                    if pending_format_count > 0:
                        await db_add_log(db, task_id, f"[INFO] {pending_format_count} 个基因组待格式化处理")
                if predownload_count is not None:
                    db_updates["predownload_count"] = predownload_count
                    if predownload_count > 0:
                        await db_add_log(db, task_id, f"[INFO] 已预下载 {predownload_count} 个基因组（暂存区待应用）")
                if db_updates:
                    set_sql = ", ".join(f"{k}=?" for k in db_updates)
                    vals = list(db_updates.values()) + [task["taxon"], task["genome_dir"]]
                    await db.execute(
                        f"UPDATE taxon_configs SET {set_sql} WHERE taxon=? AND genome_dir=?",
                        vals,
                    )
                await db_update_task(db, task_id, status="done", finished_at=finished)
                await db.commit()
            logger.info("Task %d finished successfully.", task_id)
        else:
            final_status = "cancelled" if returncode in (130, -15, -9) else "failed"
            error_msg = f"退出码: {returncode}"
            async with get_db() as db:
                if final_status == "cancelled":
                    await db_add_log(db, task_id, f"[INFO] 任务已取消，{error_msg}")
                else:
                    await db_add_log(db, task_id, f"[ERROR] 任务结束，{error_msg}")
                await db_update_task(
                    db, task_id,
                    status=final_status,
                    finished_at=finished,
                    error_msg=error_msg,
                )
            logger.warning("Task %d ended with status=%s returncode=%d.", task_id, final_status, returncode)

    async def submit(self, task_id: int) -> None:
        if task_id in self._pending or task_id in self._running:
            logger.warning("Task %d already queued or running, skipping duplicate submission.", task_id)
            return
        self._pending.add(task_id)
        await self._queue.put(task_id)
        logger.info("Task %d submitted to queue.", task_id)

    async def cancel(self, task_id: int) -> bool:
        """取消任务。
        
        Returns:
            True 如果取消了排队或运行中的任务，False 如果任务不存在或无法取消。
        """
        if task_id in _processes:
            proc = _processes[task_id]
            pgid = proc.pid  # pgid 与 pid 相同，因为 preexec_fn=os.setsid
            try:
                # Step 1: 发送 SIGTERM 到整个进程组，给所有子进程优雅关闭的机会
                try:
                    os.killpg(pgid, signal.SIGTERM)
                    logger.info("Task %d: SIGTERM sent to process group %d.", task_id, pgid)
                except ProcessLookupError:
                    logger.debug("Task %d: process group %d not found.", task_id, pgid)
                
                try:
                    await asyncio.wait_for(proc.wait(), timeout=5.0)
                    logger.info("Task %d: process group gracefully terminated.", task_id)
                    return True
                except asyncio.TimeoutError:
                    # Step 2: 如果 5 秒后仍未退出，发送 SIGKILL 强制杀死整个进程组
                    logger.warning("Task %d: SIGTERM timeout, sending SIGKILL to process group %d.", task_id, pgid)
                    try:
                        os.killpg(pgid, signal.SIGKILL)
                        await proc.wait()
                        logger.info("Task %d: process group killed with SIGKILL.", task_id)
                    except ProcessLookupError:
                        logger.debug("Task %d: process group already terminated.", task_id)
                    return True
            except Exception as exc:
                logger.error("Task %d: cancel error: %s", task_id, exc)
                return True
            finally:
                # 清理进程引用
                _processes.pop(task_id, None)

        # 已分发但正在等待 taxon 锁，或排队中尚未被 dispatcher 取走
        if task_id in self._running or task_id in self._pending:
            self._pending.discard(task_id)
            # 等待中的 asyncio.Task 会在获取锁后检查 DB 状态并自动退出
            finished = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            async with get_db() as db:
                await db_update_task(
                    db, task_id,
                    status="cancelled",
                    finished_at=finished,
                    error_msg="用户取消（排队中）",
                )
                await db_add_log(db, task_id, "[INFO] 任务已取消（排队中）")
            logger.info("Task %d cancelled while pending/waiting for taxon lock.", task_id)
            return True

        return False


# ─────────────────────────────────────────────────────────────
# AutoUpdateScheduler
# ─────────────────────────────────────────────────────────────

class AutoUpdateScheduler:
    """每小时扫描到期的 taxon_configs，自动提交更新任务。"""

    CHECK_INTERVAL = 3600  # 秒

    def __init__(self) -> None:
        self._task: Optional[asyncio.Task] = None  # type: ignore[type-arg]

    async def start(self) -> None:
        self._task = asyncio.create_task(
            self._loop(), name="auto_update_scheduler"
        )
        logger.info("AutoUpdateScheduler started (check interval: %ds).", self.CHECK_INTERVAL)

    async def _loop(self) -> None:
        # 启动后先等一段时间再首次检查，避免与应用启动竞争
        await asyncio.sleep(60)
        while True:
            try:
                await self._check_and_submit()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("AutoUpdateScheduler error: %s", exc)
            await asyncio.sleep(self.CHECK_INTERVAL)

    async def _check_and_submit(self) -> None:
        async with get_db() as db:
            due_items = await db_get_steps_due_for_update(db)

        if not due_items:
            return

        for item in due_items:
            await self._submit_config_step(item["cfg"], item["step"])

    async def _submit_config_step(self, cfg: dict, step: str) -> None:
        """为一个到期的步骤提交自动调度任务。"""
        async with get_db() as db:
            async with db.execute(
                """SELECT COUNT(*) FROM tasks
                   WHERE taxon=? AND genome_dir=? AND status IN ('pending','running')""",
                (cfg["taxon"], cfg["genome_dir"]),
            ) as cur:
                (active_count,) = await cur.fetchone()

        if active_count > 0:
            logger.info(
                "AutoUpdate skipped %s step for %s: already has active task.", step, cfg["taxon"]
            )
            return

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
            # check: 检查 | predownload: 检查+下载到staging | merge(合并更新): 从predownload/移动+更新md5/元数据 | process(格式化): 仅BLAST+归档
            "do_check":    1 if step in ("check", "predownload") else 0,
            "do_download": 1 if step in ("predownload", "merge") else 0,
            "do_process":  1 if step == "process" else 0,
        }
        interval_days = cfg.get(f"{step}_interval_days", 0)
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        next_at = (datetime.now() + timedelta(days=interval_days)).strftime("%Y-%m-%d %H:%M:%S")

        async with get_db() as db:
            task_id = await db_create_task(db, task_data)
            await db_update_taxon_config(db, cfg["id"], {
                "last_auto_updated": now_str,
                f"next_{step}_at":   next_at,
            })

        await task_queue.submit(task_id)
        logger.info(
            "AutoUpdate: submitted %s task #%d for %s (next: %s).",
            step, task_id, cfg["taxon"], next_at,
        )


# ─────────────────────────────────────────────────────────────
# 全局单例
# ─────────────────────────────────────────────────────────────
task_queue            = TaskQueue()
auto_update_scheduler = AutoUpdateScheduler()


# ─────────────────────────────────────────────────────────────
# 基因组完整性校验
# ─────────────────────────────────────────────────────────────

# 内存中的校验状态字典，key = check_id (8位 UUID)
_integrity_checks: dict[str, dict] = {}
_integrity_tasks: dict[str, asyncio.Task] = {}
_integrity_target_running: dict[str, str] = {}
_integrity_target_latest: dict[str, str] = {}


def _integrity_target_key(genome_dir: str, config_id: int | None = None) -> str:
    """将校验目标标准化为可复用的唯一 key。"""
    gdir = str(Path(genome_dir).resolve())
    return f"{config_id if config_id is not None else 'none'}::{gdir}"


def _compute_md5(path: Path) -> str:
    """计算文件 MD5（在线程池中执行，不阻塞事件循环）。"""
    hasher = hashlib.md5()
    with path.open("rb") as f:
        while chunk := f.read(1 << 20):
            hasher.update(chunk)
    return hasher.hexdigest()


async def run_integrity_check(check_id: str, genome_dir: str) -> None:
    """后台协程：读取两个 md5sums 文件，逐一校验 .fna 文件完整性。"""
    state = _integrity_checks[check_id]
    gdir  = Path(genome_dir)

    # 收集所有有 MD5 记录的条目：(gca, expected_md5, fna_path_or_None)
    entries: list[tuple[str, str, Path | None]] = []
    for md5_file, sub_dir in [
        (non_ref_md5_path(gdir), non_ref_genomes_dir(gdir)),
        (ref_md5_path(gdir),     ref_genomes_dir(gdir)),
    ]:
        if not md5_file.exists():
            continue
        for line in md5_file.open():
            parts = line.strip().split()
            # 跳过空行和占位符（MD5 长度必须为 32 位十六进制）
            if len(parts) == 2 and len(parts[1]) == 32:
                gca, expected = parts[0], parts[1]
                fna = sub_dir / f"{gca}.fna"
                entries.append((gca, expected, fna if fna.exists() else None))

    state["total"]         = len(entries)
    state["current"]       = 0
    state["passed"]        = 0
    state["failed"]        = 0
    state["missing"]       = 0
    state["current_file"]  = ""
    state["failed_files"]  = []
    state["missing_files"] = []
    workers = max(1, min(int(state.get("workers") or 1), 64))
    state["workers"] = workers

    try:
        if not entries:
            state["status"]      = "done"
            state["finished_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            return

        loop = asyncio.get_event_loop()
        progress_lock = asyncio.Lock()
        queue: asyncio.Queue[tuple[str, str, Path | None] | None] = asyncio.Queue()
        for entry in entries:
            queue.put_nowait(entry)
        for _ in range(workers):
            queue.put_nowait(None)

        executor = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="md5chk")

        async def _worker() -> None:
            while True:
                item = await queue.get()
                if item is None:
                    queue.task_done()
                    return

                gca, expected_md5, fna_path = item
                if state.get("cancel_requested"):
                    queue.task_done()
                    continue

                actual: str | None = None
                missing = False
                if fna_path is None:
                    missing = True
                else:
                    try:
                        actual = await loop.run_in_executor(executor, _compute_md5, fna_path)
                    except OSError:
                        missing = True

                async with progress_lock:
                    state["current"] += 1
                    state["current_file"] = gca
                    if missing:
                        state["missing"] += 1
                        state["missing_files"].append(gca)
                    elif actual == expected_md5:
                        state["passed"] += 1
                    else:
                        state["failed"] += 1
                        state["failed_files"].append({
                            "gca":      gca,
                            "path":     str(fna_path),
                            "expected": expected_md5,
                            "actual":   actual or "",
                        })

                queue.task_done()
                await asyncio.sleep(0)

        worker_tasks = [asyncio.create_task(_worker()) for _ in range(workers)]
        try:
            await queue.join()
            await asyncio.gather(*worker_tasks)
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

        if state.get("cancel_requested"):
            state["status"] = "cancelled"
            state["current_file"] = ""
            state["finished_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            return

        state["status"]       = "done"
        state["current_file"] = ""
        state["finished_at"]  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    except asyncio.CancelledError:
        state["status"] = "cancelled"
        state["current_file"] = ""
        state["finished_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        raise
    except Exception as exc:
        state["status"] = "failed"
        state["current_file"] = ""
        state["error"] = str(exc)
        state["finished_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    finally:
        _integrity_tasks.pop(check_id, None)
        target_key = state.get("target_key")
        if target_key and _integrity_target_running.get(target_key) == check_id:
            _integrity_target_running.pop(target_key, None)


def start_integrity_check_task(
    genome_dir: str,
    config_id: int | None = None,
    reuse_running: bool = True,
    force_new: bool = False,
    workers: int = 1,
) -> tuple[str, bool]:
    """注册并启动完整性校验后台任务，返回 (check_id, reused)。"""
    target_key = _integrity_target_key(genome_dir, config_id)

    # 默认行为：优先复用该目标最近一次校验（无论 running / done / cancelled / failed）
    if not force_new:
        latest_id = _integrity_target_latest.get(target_key)
        if latest_id and latest_id in _integrity_checks:
            return latest_id, True

    if not force_new and reuse_running:
        running_id = _integrity_target_running.get(target_key)
        if running_id:
            state = _integrity_checks.get(running_id)
            if state and state.get("status") in ("running", "cancelling"):
                return running_id, True

    check_id = str(uuid.uuid4())[:8]
    _integrity_checks[check_id] = {
        "check_id":    check_id,
        "genome_dir":  str(Path(genome_dir).resolve()),
        "config_id":   config_id,
        "target_key":  target_key,
        "status":      "running",
        "cancel_requested": False,
        "workers":     max(1, min(int(workers), 64)),
        "started_at":  datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "finished_at": None,
        "total":       0,
        "current":     0,
        "passed":      0,
        "failed":      0,
        "missing":     0,
        "current_file": "",
        "failed_files":  [],
        "missing_files": [],
    }
    task = asyncio.create_task(
        run_integrity_check(check_id, genome_dir),
        name=f"integrity_check_{check_id}",
    )
    _integrity_tasks[check_id] = task
    _integrity_target_running[target_key] = check_id
    _integrity_target_latest[target_key] = check_id
    return check_id, False


def cancel_integrity_check_task(check_id: str) -> bool:
    """取消正在运行的完整性校验任务。"""
    state = _integrity_checks.get(check_id)
    if state is None:
        return False
    if state.get("status") != "running":
        return False
    state["cancel_requested"] = True
    state["status"] = "cancelling"
    task = _integrity_tasks.get(check_id)
    if task and not task.done():
        task.cancel()
    return True


# ─────────────────────────────────────────────────────────────
# BLAST 数据库完整性校验
# ─────────────────────────────────────────────────────────────

_blast_db_checks: dict[str, dict] = {}
_blast_db_tasks: dict[str, asyncio.Task] = {}
_blast_db_target_running: dict[str, str] = {}
_blast_db_target_latest: dict[str, str] = {}


def _blast_db_target_key(genome_dir: str, config_id: int | None = None) -> str:
    gdir = str(Path(genome_dir).resolve())
    return f"blastdb:{config_id if config_id is not None else 'none'}::{gdir}"


async def run_blast_db_check(check_id: str, genome_dir: str) -> None:
    """后台协程：扫描已建库数据库，用 blastdbcmd -info 逐一校验，记录失败项名单。

    简化流程（不再做 MD5 核验或自动重建）：
    1. 扫描：找出所有已有 .nhr 文件的基因组
    2. 校验：并行执行 blastdbcmd -info，失败的加入 failed_gcas 列表
    """
    from genome_downloader.processor import _check_db_valid

    state = _blast_db_checks[check_id]
    gdir  = Path(genome_dir)
    workers = max(1, min(int(state.get("workers") or 1), 64))
    state["workers"] = workers

    executor = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="blastdbchk")
    progress_lock = asyncio.Lock()
    loop = asyncio.get_event_loop()

    try:
        # ── Phase 1: 扫描 ────────────────────────────────────────────────────
        state["phase"] = "scanning"
        to_check: list[tuple[Path, Path]] = []
        scan_pairs = [
            (non_ref_genomes_dir(gdir), non_ref_blastdb_dir(gdir)),
            (ref_genomes_dir(gdir),     ref_blastdb_dir(gdir)),
        ]
        for scan_dir, blastdb_dir in scan_pairs:
            if not scan_dir.exists():
                continue
            for fna in sorted(scan_dir.glob("*.fna")):
                nhr_file = blastdb_dir / (fna.stem + ".nhr")
                if nhr_file.exists():
                    to_check.append((fna, blastdb_dir / fna.stem))

        state["total"] = len(to_check)
        if not to_check:
            state["status"]      = "done"
            state["phase"]       = "done"
            state["finished_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            return

        # ── Phase 2: blastdbcmd -info 并行校验 ───────────────────────────────
        state["phase"] = "validating"

        async def _validate_one(fna: Path, db_prefix: Path) -> None:
            if state.get("cancel_requested"):
                return
            gca, ok, err = await loop.run_in_executor(executor, _check_db_valid, fna, db_prefix)
            async with progress_lock:
                state["current"] += 1
                if ok:
                    state["valid"] += 1
                else:
                    state["invalid"] += 1
                    state["failed_gcas"].append({"gca": gca, "error": err})

        validate_tasks = [asyncio.create_task(_validate_one(fna, dp)) for fna, dp in to_check]
        await asyncio.gather(*validate_tasks, return_exceptions=True)

        if state.get("cancel_requested"):
            state["status"] = "cancelled"
        else:
            state["status"] = "done"
        state["phase"]       = "done"
        state["finished_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    except asyncio.CancelledError:
        state["status"]      = "cancelled"
        state["phase"]       = "done"
        state["finished_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        raise
    except Exception as exc:
        state["status"]      = "failed"
        state["phase"]       = "done"
        state["error"]       = str(exc)
        state["finished_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    finally:
        executor.shutdown(wait=False, cancel_futures=True)
        _blast_db_tasks.pop(check_id, None)
        target_key = state.get("target_key")
        if target_key and _blast_db_target_running.get(target_key) == check_id:
            _blast_db_target_running.pop(target_key, None)


def start_blast_db_check_task(
    genome_dir: str,
    config_id: int | None = None,
    reuse_running: bool = True,
    force_new: bool = False,
    workers: int = 1,
) -> tuple[str, bool]:
    """注册并启动 BLAST 数据库完整性校验后台任务，返回 (check_id, reused)。"""
    target_key = _blast_db_target_key(genome_dir, config_id)

    if not force_new:
        latest_id = _blast_db_target_latest.get(target_key)
        if latest_id and latest_id in _blast_db_checks:
            return latest_id, True

    if not force_new and reuse_running:
        running_id = _blast_db_target_running.get(target_key)
        if running_id:
            s = _blast_db_checks.get(running_id)
            if s and s.get("status") in ("running", "cancelling"):
                return running_id, True

    check_id = str(uuid.uuid4())[:8]
    _blast_db_checks[check_id] = {
        "check_id":        check_id,
        "genome_dir":      str(Path(genome_dir).resolve()),
        "config_id":       config_id,
        "target_key":      target_key,
        "status":          "running",
        "cancel_requested": False,
        "workers":         max(1, min(int(workers), 64)),
        "phase":           "scanning",
        "started_at":      datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "finished_at":     None,
        "total":           0,
        "current":         0,
        "valid":           0,
        "invalid":         0,
        "failed_gcas":     [],
    }
    task = asyncio.create_task(
        run_blast_db_check(check_id, genome_dir),
        name=f"blast_db_check_{check_id}",
    )
    _blast_db_tasks[check_id] = task
    _blast_db_target_running[target_key] = check_id
    _blast_db_target_latest[target_key]  = check_id
    return check_id, False


def cancel_blast_db_check_task(check_id: str) -> bool:
    """取消正在运行的 BLAST 数据库校验任务。"""
    state = _blast_db_checks.get(check_id)
    if state is None:
        return False
    if state.get("status") != "running":
        return False
    state["cancel_requested"] = True
    state["status"] = "cancelling"
    task = _blast_db_tasks.get(check_id)
    if task and not task.done():
        task.cancel()
    return True


# ─────────────────────────────────────────────────────────────
# FAI 索引构建任务
# ─────────────────────────────────────────────────────────────

_fai_tasks_state: dict[str, dict] = {}
_fai_tasks_bg: dict[str, asyncio.Task] = {}
_fai_target_latest: dict[str, str] = {}


async def run_fai_index_task(task_id: str, genome_dir: str) -> None:
    """后台协程：为 genome_dir 下所有缺少 .fai 的 .fna 文件运行 samtools faidx。"""
    from genome_downloader.processor import _build_fai_one

    state = _fai_tasks_state[task_id]
    gdir = Path(genome_dir)
    workers = max(1, min(int(state.get("workers") or 1), 64))
    state["workers"] = workers

    executor = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="fai_idx")
    progress_lock = asyncio.Lock()
    loop = asyncio.get_event_loop()

    try:
        # 收集需要构建索引的文件
        to_build: list[Path] = []
        skipped = 0
        for scan_dir in (non_ref_genomes_dir(gdir), ref_genomes_dir(gdir)):
            if not scan_dir.exists():
                continue
            for fna in sorted(scan_dir.glob("*.fna")):
                if Path(str(fna) + ".fai").exists():
                    skipped += 1
                else:
                    to_build.append(fna)

        total = len(to_build) + skipped
        state["total"]   = total
        state["skipped"] = skipped

        if not to_build:
            state["status"]      = "done"
            state["finished_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            return

        async def _index_one(fna: Path) -> None:
            if state.get("cancel_requested"):
                return
            gca, ok, err = await loop.run_in_executor(executor, _build_fai_one, fna)
            async with progress_lock:
                state["current"] += 1
                if ok:
                    state["built"] += 1
                else:
                    state["failed_gcas"].append({"gca": gca, "error": err})

        index_tasks = [asyncio.create_task(_index_one(fna)) for fna in to_build]
        await asyncio.gather(*index_tasks, return_exceptions=True)

        if state.get("cancel_requested"):
            state["status"] = "cancelled"
        else:
            state["status"] = "done"
        state["finished_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    except asyncio.CancelledError:
        state["status"]      = "cancelled"
        state["finished_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        raise
    except Exception as exc:
        state["status"]      = "failed"
        state["error"]       = str(exc)
        state["finished_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    finally:
        executor.shutdown(wait=False, cancel_futures=True)
        _fai_tasks_bg.pop(task_id, None)


def start_fai_index_task(
    genome_dir: str,
    workers: int = 1,
) -> tuple[str, bool]:
    """注册并启动 FAI 索引构建后台任务，返回 (task_id, reused)。

    如果该目录已有运行中任务则复用。
    """
    target_key = str(Path(genome_dir).resolve())

    # 复用最近一次（含已完成）
    latest_id = _fai_target_latest.get(target_key)
    if latest_id and latest_id in _fai_tasks_state:
        s = _fai_tasks_state[latest_id]
        if s.get("status") in ("running", "cancelling"):
            return latest_id, True

    task_id = str(uuid.uuid4())[:8]
    _fai_tasks_state[task_id] = {
        "task_id":          task_id,
        "genome_dir":       str(Path(genome_dir).resolve()),
        "status":           "running",
        "cancel_requested": False,
        "workers":          max(1, min(int(workers), 64)),
        "started_at":       datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "finished_at":      None,
        "total":            0,
        "current":          0,
        "built":            0,
        "skipped":          0,
        "failed_gcas":      [],
        "error":            None,
    }
    bg_task = asyncio.create_task(
        run_fai_index_task(task_id, genome_dir),
        name=f"fai_index_{task_id}",
    )
    _fai_tasks_bg[task_id] = bg_task
    _fai_target_latest[target_key] = task_id
    return task_id, False


def cancel_fai_index_task(task_id: str) -> bool:
    """取消正在运行的 FAI 索引构建任务。"""
    state = _fai_tasks_state.get(task_id)
    if state is None:
        return False
    if state.get("status") != "running":
        return False
    state["cancel_requested"] = True
    state["status"] = "cancelling"
    bg = _fai_tasks_bg.get(task_id)
    if bg and not bg.done():
        bg.cancel()
    return True
