#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库模块 — SQLite (via aiosqlite)
包含数据库初始化、连接管理和所有表的 CRUD 操作。
"""
from __future__ import annotations

import aiosqlite
import hashlib
import hmac
import secrets
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator

DB_PATH = Path(__file__).parent / "genome_manager.db"

# ─────────────────────────────────────────────────────────────
# DDL — 建表 & 索引
# ─────────────────────────────────────────────────────────────
_DDL = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS tasks (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    taxon               TEXT    NOT NULL,
    genome_type         TEXT    NOT NULL DEFAULT 'ref',
    genome_dir          TEXT    NOT NULL,
    tmp_dir             TEXT,
    api_key             TEXT,
    threads             INTEGER NOT NULL DEFAULT 4,
    batch_size          INTEGER NOT NULL DEFAULT 500,
    parallel_downloads  INTEGER NOT NULL DEFAULT 4,
    overwrite           INTEGER NOT NULL DEFAULT 0,
    do_check            INTEGER NOT NULL DEFAULT 1,
    do_download         INTEGER NOT NULL DEFAULT 1,
    do_process          INTEGER NOT NULL DEFAULT 1,
    do_validate_db      INTEGER NOT NULL DEFAULT 0,
    status              TEXT    NOT NULL DEFAULT 'pending',
    created_at          TEXT    DEFAULT (datetime('now','localtime')),
    started_at          TEXT,
    finished_at         TEXT,
    pid                 INTEGER,
    error_msg           TEXT
);

CREATE TABLE IF NOT EXISTS task_logs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id     INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    content     TEXT    NOT NULL,
    created_at  TEXT    DEFAULT (datetime('now','localtime'))
);

CREATE TABLE IF NOT EXISTS genomes (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    gca          TEXT    NOT NULL UNIQUE,
    organism     TEXT,
    length       INTEGER,
    taxid        TEXT,
    lineage      TEXT,
    genome_type  TEXT,
    genome_dir   TEXT,
    taxon        TEXT,
    last_updated TEXT    DEFAULT (datetime('now','localtime'))
);

CREATE TABLE IF NOT EXISTS taxon_configs (
    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
    name                   TEXT    NOT NULL,
    taxon                  TEXT    NOT NULL,
    genome_type            TEXT    NOT NULL DEFAULT 'ref',
    genome_dir             TEXT    NOT NULL,
    tmp_dir                TEXT,
    threads                INTEGER NOT NULL DEFAULT 4,
    batch_size             INTEGER NOT NULL DEFAULT 500,
    parallel_downloads     INTEGER NOT NULL DEFAULT 4,
    api_key                TEXT,
    check_interval_days    INTEGER NOT NULL DEFAULT 0,
    download_interval_days INTEGER NOT NULL DEFAULT 0,
    process_interval_days  INTEGER NOT NULL DEFAULT 0,
    last_auto_updated      TEXT,
    next_check_at          TEXT,
    next_download_at       TEXT,
    next_process_at        TEXT,
    pending_count          INTEGER NOT NULL DEFAULT 0,
    pending_format_count   INTEGER NOT NULL DEFAULT 0,
    predownload_count      INTEGER NOT NULL DEFAULT 0,
    predownload_interval_days INTEGER NOT NULL DEFAULT 0,
    next_predownload_at    TEXT,
    genome_date            TEXT,
    do_check               INTEGER NOT NULL DEFAULT 1,
    do_download            INTEGER NOT NULL DEFAULT 1,
    do_process             INTEGER NOT NULL DEFAULT 1,
    overwrite              INTEGER NOT NULL DEFAULT 0,
    icon                   TEXT,
    created_at             TEXT    DEFAULT (datetime('now','localtime')),
    updated_at             TEXT    DEFAULT (datetime('now','localtime'))
);

CREATE TABLE IF NOT EXISTS settings (
    key    TEXT PRIMARY KEY,
    value  TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tasks_status       ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_tasks_created      ON tasks(created_at);
CREATE INDEX IF NOT EXISTS idx_task_logs_task_id  ON task_logs(task_id);
CREATE INDEX IF NOT EXISTS idx_genomes_taxid      ON genomes(taxid);
CREATE INDEX IF NOT EXISTS idx_genomes_genome_dir ON genomes(genome_dir);
"""


async def init_db() -> None:
    """初始化数据库，创建所有表和索引（幂等）。"""
    async with aiosqlite.connect(str(DB_PATH)) as db:
        await db.executescript(_DDL)
        await db.commit()
        # 迁移：为已有数据库补充 icon 列
        async with db.execute("PRAGMA table_info(taxon_configs)") as cur:
            cols = {row[1] async for row in cur}
        if "icon" not in cols:
            await db.execute("ALTER TABLE taxon_configs ADD COLUMN icon TEXT")
            await db.commit()


@asynccontextmanager
async def get_db() -> AsyncIterator[aiosqlite.Connection]:
    """异步数据库连接上下文管理器。

    用法::

        async with get_db() as db:
            rows = await db_list_tasks(db)
    """
    async with aiosqlite.connect(str(DB_PATH)) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("PRAGMA foreign_keys=ON")
        yield db


# ─────────────────────────────────────────────────────────────
# Tasks CRUD
# ─────────────────────────────────────────────────────────────

async def db_create_task(db: aiosqlite.Connection, data: dict) -> int:
    cursor = await db.execute(
        """INSERT INTO tasks
               (taxon, genome_type, genome_dir, tmp_dir, api_key,
                threads, batch_size, parallel_downloads, overwrite,
                do_check, do_download, do_process, do_validate_db)
           VALUES
               (:taxon, :genome_type, :genome_dir, :tmp_dir, :api_key,
                :threads, :batch_size, :parallel_downloads, :overwrite,
                :do_check, :do_download, :do_process, :do_validate_db)""",
        {
            "taxon":               data["taxon"],
            "genome_type":         data["genome_type"],
            "genome_dir":          data["genome_dir"],
            "tmp_dir":             data.get("tmp_dir"),
            "api_key":             data.get("api_key"),
            "threads":             data.get("threads", 4),
            "batch_size":          data.get("batch_size", 500),
            "parallel_downloads":  data.get("parallel_downloads", 4),
            "overwrite":           int(bool(data.get("overwrite", False))),
            "do_check":            int(bool(data.get("do_check", True))),
            "do_download":         int(bool(data.get("do_download", True))),
            "do_process":          int(bool(data.get("do_process", True))),
            "do_validate_db":      int(bool(data.get("do_validate_db", False))),
        },
    )
    await db.commit()
    return cursor.lastrowid  # type: ignore[return-value]


async def db_get_task(db: aiosqlite.Connection, task_id: int) -> dict | None:
    async with db.execute("SELECT * FROM tasks WHERE id=?", (task_id,)) as cur:
        row = await cur.fetchone()
    return dict(row) if row else None


async def db_list_tasks(
    db: aiosqlite.Connection,
    status: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[dict]:
    if status:
        sql = "SELECT * FROM tasks WHERE status=? ORDER BY id DESC LIMIT ? OFFSET ?"
        params: tuple = (status, limit, offset)
    else:
        sql = "SELECT * FROM tasks ORDER BY id DESC LIMIT ? OFFSET ?"
        params = (limit, offset)
    async with db.execute(sql, params) as cur:
        rows = await cur.fetchall()
    return [dict(r) for r in rows]


async def db_count_tasks(
    db: aiosqlite.Connection, status: str | None = None
) -> int:
    if status:
        sql, params = "SELECT COUNT(*) FROM tasks WHERE status=?", (status,)
    else:
        sql, params = "SELECT COUNT(*) FROM tasks", ()
    async with db.execute(sql, params) as cur:  # type: ignore[arg-type]
        row = await cur.fetchone()
    return row[0]  # type: ignore[index]


async def db_update_task(
    db: aiosqlite.Connection, task_id: int, **kwargs: object
) -> None:
    if not kwargs:
        return
    set_clause = ", ".join(f"{k}=?" for k in kwargs)
    values = [*kwargs.values(), task_id]
    await db.execute(f"UPDATE tasks SET {set_clause} WHERE id=?", values)
    await db.commit()


async def db_delete_tasks(
    db: aiosqlite.Connection, task_ids: list[int]
) -> int:
    """删除指定任务及其所有日志记录，返回实际删除的任务数量。"""
    if not task_ids:
        return 0
    placeholders = ",".join("?" * len(task_ids))
    await db.execute(
        f"DELETE FROM task_logs WHERE task_id IN ({placeholders})", task_ids
    )
    cursor = await db.execute(
        f"DELETE FROM tasks WHERE id IN ({placeholders})", task_ids
    )
    await db.commit()
    return cursor.rowcount  # type: ignore[return-value]


# ─────────────────────────────────────────────────────────────
# Task Logs CRUD
# ─────────────────────────────────────────────────────────────

async def db_add_log(
    db: aiosqlite.Connection, task_id: int, content: str
) -> int:
    if len(content) > 4096:
        content = content[:4093] + "..."
    cursor = await db.execute(
        "INSERT INTO task_logs (task_id, content) VALUES (?,?)",
        (task_id, content),
    )
    await db.commit()
    return cursor.lastrowid  # type: ignore[return-value]


async def db_get_logs(
    db: aiosqlite.Connection, task_id: int, after_id: int = 0
) -> list[dict]:
    async with db.execute(
        "SELECT * FROM task_logs WHERE task_id=? AND id>? ORDER BY id ASC",
        (task_id, after_id),
    ) as cur:
        rows = await cur.fetchall()
    return [dict(r) for r in rows]


# ─────────────────────────────────────────────────────────────
# Genomes CRUD
# ─────────────────────────────────────────────────────────────

async def db_upsert_genome(db: aiosqlite.Connection, data: dict) -> None:
    await db.execute(
        """INSERT INTO genomes
               (gca, organism, length, taxid, lineage, genome_type, genome_dir, taxon, last_updated)
           VALUES
               (:gca, :organism, :length, :taxid, :lineage,
                :genome_type, :genome_dir, :taxon, datetime('now','localtime'))
           ON CONFLICT(gca) DO UPDATE SET
               organism     = COALESCE(excluded.organism,    organism),
               length       = COALESCE(excluded.length,      length),
               taxid        = COALESCE(excluded.taxid,       taxid),
               lineage      = COALESCE(excluded.lineage,     lineage),
               genome_type  = COALESCE(excluded.genome_type, genome_type),
               genome_dir   = excluded.genome_dir,
               taxon        = COALESCE(excluded.taxon,       taxon),
               last_updated = excluded.last_updated""",
        data,
    )


def _build_search_conditions(
    search: str | None,
    conditions: list[str],
    params: list,
) -> None:
    """将搜索词展开为空格/下划线双形式的 LIKE 条件，就地修改 conditions 和 params。"""
    if not search:
        return
    search_variants: list[str] = []
    normalized = " ".join(search.strip().split())
    if normalized:
        search_variants.append(normalized)
    if " " in normalized:
        search_variants.append(normalized.replace(" ", "_"))
    if "_" in normalized:
        search_variants.append(normalized.replace("_", " "))
    search_variants = list(dict.fromkeys(search_variants))

    sub_conditions: list[str] = []
    for pat in search_variants:
        like_pat = f"%{pat}%"
        sub_conditions.append(
            "(gca LIKE ? OR organism LIKE ? OR REPLACE(organism, '_', ' ') LIKE ? OR lineage LIKE ? OR taxid LIKE ?)"
        )
        params += [like_pat, like_pat, like_pat, like_pat, like_pat]
    conditions.append("(" + " OR ".join(sub_conditions) + ")")


async def db_list_genomes(
    db: aiosqlite.Connection,
    search: str | None = None,
    genome_type: str | None = None,
    genome_dir: str | None = None,
    limit: int = 50,
    offset: int = 0,
    taxon: str | None = None,
) -> list[dict]:
    conditions: list[str] = []
    params: list = []
    _build_search_conditions(search, conditions, params)
    if genome_type:
        conditions.append("genome_type=?")
        params.append(genome_type)
    if genome_dir:
        conditions.append("genome_dir=?")
        params.append(genome_dir)
    if taxon:
        conditions.append("taxon=?")
        params.append(taxon)
    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
    params += [limit, offset]
    async with db.execute(
        f"SELECT * FROM genomes{where} ORDER BY length DESC LIMIT ? OFFSET ?",
        params,
    ) as cur:
        rows = await cur.fetchall()
    return [dict(r) for r in rows]


async def db_count_genomes(
    db: aiosqlite.Connection,
    search: str | None = None,
    genome_type: str | None = None,
    genome_dir: str | None = None,
    taxon: str | None = None,
) -> int:
    conditions: list[str] = []
    params: list = []
    _build_search_conditions(search, conditions, params)
    if genome_type:
        conditions.append("genome_type=?")
        params.append(genome_type)
    if genome_dir:
        conditions.append("genome_dir=?")
        params.append(genome_dir)
    if taxon:
        conditions.append("taxon=?")
        params.append(taxon)
    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
    async with db.execute(
        f"SELECT COUNT(*) FROM genomes{where}", params
    ) as cur:
        row = await cur.fetchone()
    return row[0]  # type: ignore[index]


# ─────────────────────────────────────────────────────────────
# Taxon Configs CRUD
# ─────────────────────────────────────────────────────────────

async def db_create_taxon_config(
    db: aiosqlite.Connection, data: dict
) -> int:
    cursor = await db.execute(
        """INSERT INTO taxon_configs
               (name, taxon, genome_type, genome_dir, tmp_dir,
                threads, batch_size, parallel_downloads, api_key,
                check_interval_days, predownload_interval_days,
                download_interval_days, process_interval_days,
                next_check_at, next_predownload_at, next_download_at, next_process_at,
                do_check, do_download, do_process, overwrite, genome_date, icon)
           VALUES
               (:name, :taxon, :genome_type, :genome_dir, :tmp_dir,
                :threads, :batch_size, :parallel_downloads, :api_key,
                :check_interval_days, :predownload_interval_days,
                :download_interval_days, :process_interval_days,
                :next_check_at, :next_predownload_at, :next_download_at, :next_process_at,
                :do_check, :do_download, :do_process, :overwrite, :genome_date, :icon)""",
        {
            "name":                      data["name"],
            "taxon":                     data["taxon"],
            "genome_type":               data["genome_type"],
            "genome_dir":                data["genome_dir"],
            "tmp_dir":                   data.get("tmp_dir"),
            "threads":                   data.get("threads", 4),
            "batch_size":                data.get("batch_size", 500),
            "parallel_downloads":        data.get("parallel_downloads", 4),
            "api_key":                   data.get("api_key"),
            "check_interval_days":       int(data.get("check_interval_days", 0)),
            "predownload_interval_days": int(data.get("predownload_interval_days", 0)),
            "download_interval_days":    int(data.get("download_interval_days", 0)),
            "process_interval_days":     int(data.get("process_interval_days", 0)),
            "next_check_at":             data.get("next_check_at"),
            "next_predownload_at":        data.get("next_predownload_at"),
            "next_download_at":          data.get("next_download_at"),
            "next_process_at":           data.get("next_process_at"),
            "do_check":                  int(bool(data.get("do_check", True))),
            "do_download":               int(bool(data.get("do_download", True))),
            "do_process":                int(bool(data.get("do_process", True))),
            "overwrite":                 int(bool(data.get("overwrite", False))),
            "genome_date":               data.get("genome_date") or None,
            "icon":                      data.get("icon") or None,
        },
    )
    await db.commit()
    return cursor.lastrowid  # type: ignore[return-value]


async def db_list_taxon_configs(
    db: aiosqlite.Connection,
) -> list[dict]:
    async with db.execute(
        "SELECT * FROM taxon_configs ORDER BY id DESC"
    ) as cur:
        rows = await cur.fetchall()
    return [dict(r) for r in rows]


async def db_get_taxon_config(
    db: aiosqlite.Connection, config_id: int
) -> dict | None:
    async with db.execute(
        "SELECT * FROM taxon_configs WHERE id=?", (config_id,)
    ) as cur:
        row = await cur.fetchone()
    return dict(row) if row else None


async def db_update_taxon_config(
    db: aiosqlite.Connection, config_id: int, data: dict
) -> None:
    set_parts = [f"{k}=?" for k in data]
    set_parts.append("updated_at=datetime('now','localtime')")
    values = [*data.values(), config_id]
    await db.execute(
        f"UPDATE taxon_configs SET {', '.join(set_parts)} WHERE id=?", values
    )
    await db.commit()


async def db_delete_taxon_config(
    db: aiosqlite.Connection, config_id: int
) -> None:
    await db.execute("DELETE FROM taxon_configs WHERE id=?", (config_id,))
    await db.commit()


# ─────────────────────────────────────────────────────────────
# Genome Groups
# ─────────────────────────────────────────────────────────────

async def db_get_genome_groups(db: aiosqlite.Connection) -> list[dict]:
    """按 (taxon, genome_dir) 分组统计基因组，左连接配置表。
    通过 UNION 包含尚无基因组数据的配置，确保基于配置展示。
    genome_type 取自配置表（代表该目录的整体类型），同时统计 ref 数量。
    """
    async with db.execute("""
        SELECT
            COALESCE(g.taxon, '(未分类)')      AS taxon,
            COALESCE(tc.genome_type, 'all')    AS genome_type,
            g.genome_dir,
            COUNT(*)                           AS count,
            COALESCE(SUM(CASE WHEN g.genome_type='ref' THEN 1 ELSE 0 END), 0) AS ref_count,
            COALESCE(SUM(g.length), 0)         AS total_length,
            MAX(g.last_updated)                AS last_updated,
            tc.id                              AS config_id,
            tc.name                            AS config_name,
            COALESCE(tc.check_interval_days, 0)    AS check_interval_days,
            COALESCE(tc.download_interval_days, 0) AS download_interval_days,
            COALESCE(tc.process_interval_days, 0)  AS process_interval_days,
            tc.next_check_at,
            tc.next_download_at,
            tc.next_process_at,
            tc.last_auto_updated,
            COALESCE(tc.pending_count, 0)      AS pending_count,
            COALESCE(tc.pending_format_count, 0) AS pending_format_count
        FROM genomes g
        LEFT JOIN taxon_configs tc
               ON tc.taxon      = g.taxon
              AND tc.genome_dir = g.genome_dir
        GROUP BY g.taxon, g.genome_dir
        UNION
        SELECT
            tc.taxon,
            tc.genome_type,
            tc.genome_dir,
            0    AS count,
            0    AS ref_count,
            0    AS total_length,
            NULL AS last_updated,
            tc.id   AS config_id,
            tc.name AS config_name,
            COALESCE(tc.check_interval_days, 0)    AS check_interval_days,
            COALESCE(tc.download_interval_days, 0) AS download_interval_days,
            COALESCE(tc.process_interval_days, 0)  AS process_interval_days,
            tc.next_check_at,
            tc.next_download_at,
            tc.next_process_at,
            tc.last_auto_updated,
            COALESCE(tc.pending_count, 0) AS pending_count,
            COALESCE(tc.pending_format_count, 0) AS pending_format_count
        FROM taxon_configs tc
        WHERE NOT EXISTS (
            SELECT 1 FROM genomes g
            WHERE g.taxon = tc.taxon
              AND g.genome_dir = tc.genome_dir
        )
        ORDER BY taxon
    """) as cur:
        rows = await cur.fetchall()
    return [dict(r) for r in rows]


async def db_get_steps_due_for_update(db: aiosqlite.Connection) -> list[dict]:
    """返回各步骤到期的配置，每项包含 'cfg'(配置 dict) 和 'step'(步骤名) 键。
    check 步骤已与 predownload 合并，调度时仅使用 predownload_interval_days。
    """
    result: list[dict] = []
    for step, interval_col, next_col in (
        ("predownload", "predownload_interval_days",  "next_predownload_at"),
        ("download",    "download_interval_days",     "next_download_at"),
        ("process",     "process_interval_days",      "next_process_at"),
    ):
        async with db.execute(f"""
            SELECT * FROM taxon_configs
            WHERE {interval_col} > 0
              AND ({next_col} IS NULL OR {next_col} <= datetime('now','localtime'))
        """) as cur:
            for row in await cur.fetchall():
                result.append({"cfg": dict(row), "step": step})
    return result


# ─────────────────────────────────────────────────────────────
# Admin Credentials (存储在 settings 表中)
# ─────────────────────────────────────────────────────────────

def _hash_password(password: str) -> str:
    """使用 PBKDF2-HMAC-SHA256 对密码哈希，返回 '{salt}:{hex_hash}' 格式。"""
    salt = secrets.token_hex(16)
    dk = hashlib.pbkdf2_hmac(
        "sha256", password.encode("utf-8"), salt.encode("utf-8"), 260_000
    )
    return f"{salt}:{dk.hex()}"


def _verify_password(password: str, stored_hash: str) -> bool:
    """验证密码是否匹配存储的哈希值（恒定时间比较，防止时序攻击）。"""
    try:
        salt, hex_hash = stored_hash.split(":", 1)
    except ValueError:
        return False
    dk = hashlib.pbkdf2_hmac(
        "sha256", password.encode("utf-8"), salt.encode("utf-8"), 260_000
    )
    return hmac.compare_digest(dk.hex(), hex_hash)


async def db_is_admin_configured(db: aiosqlite.Connection) -> bool:
    """检查是否已通过首次设置配置了管理员账号。"""
    async with db.execute(
        "SELECT value FROM settings WHERE key='admin_username'"
    ) as cur:
        row = await cur.fetchone()
    return row is not None


async def db_set_admin_credentials(
    db: aiosqlite.Connection, username: str, password: str
) -> None:
    """保存管理员账号和密码哈希（仅首次设置或修改密码时调用）。"""
    await db.execute(
        "INSERT OR REPLACE INTO settings (key, value) VALUES ('admin_username', ?)",
        (username,),
    )
    await db.execute(
        "INSERT OR REPLACE INTO settings (key, value) VALUES ('admin_password_hash', ?)",
        (_hash_password(password),),
    )
    await db.commit()


async def db_verify_admin_credentials(
    db: aiosqlite.Connection, username: str, password: str
) -> bool:
    """验证用户名和密码，均正确时返回 True。"""
    async with db.execute(
        "SELECT value FROM settings WHERE key='admin_username'"
    ) as cur:
        row = await cur.fetchone()
    if row is None or row[0] != username:
        return False
    async with db.execute(
        "SELECT value FROM settings WHERE key='admin_password_hash'"
    ) as cur:
        row = await cur.fetchone()
    if row is None:
        return False
    return _verify_password(password, row[0])
