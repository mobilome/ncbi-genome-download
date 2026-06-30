#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pydantic 数据模型 — 用于 API 请求/响应校验。
"""
from __future__ import annotations

from typing import Literal, Optional
from pydantic import BaseModel, Field, field_validator


# ─────────────────────────────────────────────────────────────
class _PathStripMixin(BaseModel):
    """Mixin：自动去除路径字段首尾空白。"""

    @field_validator("genome_dir", "tmp_dir", mode="before", check_fields=False)
    @classmethod
    def _strip_path(cls, v: object) -> object:
        if isinstance(v, str):
            stripped = v.strip()
            return stripped if stripped else None
        return v


# ─────────────────────────────────────────────────────────────
# Task 模型
# ─────────────────────────────────────────────────────────────

class TaskCreate(_PathStripMixin):
    taxon: str = Field(..., min_length=1, description="分类单元，如 fungi、bacteria")
    genome_type: Literal["ref", "all"] = Field("ref", description="基因组类型：参考(ref) 或 全部(all)")
    genome_dir: str = Field(..., min_length=1, description="基因组本地存储目录")
    tmp_dir: Optional[str] = Field(None, description="临时工作目录，默认使用当前目录")
    api_key: Optional[str] = Field(None, description="NCBI API Key（可选，提升请求频率限制）")
    threads: int = Field(4, ge=1, le=64, description="并行线程数")
    batch_size: int = Field(500, ge=1, description="Taxonomy 下载每批 TaxID 数量")
    parallel_downloads: int = Field(4, ge=1, le=16, description="Taxonomy 并行下载批次数")
    overwrite: bool = Field(False, description="强制覆盖已有下载文件")
    do_check: bool = Field(True, description="执行更新检查步骤")
    do_download: bool = Field(True, description="执行基因组下载步骤")
    do_process: bool = Field(True, description="执行格式化处理步骤")
    do_validate_db: bool = Field(False, description="校验已建库的 BLAST 数据库完整性")


class TaskResponse(BaseModel):
    id: int
    taxon: str
    genome_type: str
    genome_dir: str
    tmp_dir: Optional[str]
    threads: int
    batch_size: int
    parallel_downloads: int
    overwrite: bool
    do_check: bool
    do_download: bool
    do_process: bool
    do_validate_db: bool
    status: str
    created_at: str
    started_at: Optional[str]
    finished_at: Optional[str]
    pid: Optional[int]
    error_msg: Optional[str]


class TaskLogEntry(BaseModel):
    id: int
    task_id: int
    content: str
    created_at: str


# ─────────────────────────────────────────────────────────────
# Taxon Config 模型
# ─────────────────────────────────────────────────────────────

class TaxonConfigCreate(_PathStripMixin):
    name: str = Field(..., min_length=1, description="配置名称，如 'fungi-ref'")
    taxon: str = Field(..., min_length=1, description="分类单元")
    genome_type: Literal["ref", "all"] = "ref"
    genome_dir: str = Field(..., min_length=1, description="基因组存储目录")
    tmp_dir: Optional[str] = None
    threads: int = Field(4, ge=1, le=64)
    batch_size: int = Field(500, ge=1)
    parallel_downloads: int = Field(4, ge=1, le=16)
    api_key: Optional[str] = None
    check_interval_days:       int = Field(0, ge=0, description="检查更新自动周期（天），0=禁用")
    predownload_interval_days: int = Field(0, ge=0, description="检查+预下载自动周期（天）")
    download_interval_days:    int = Field(0, ge=0, description="合并更新自动周期（天）")
    process_interval_days:     int = Field(0, ge=0, description="格式化处理自动周期（天）")
    do_check: bool = Field(True, description="执行更新检查步骤")
    do_download: bool = Field(True, description="执行基因组下载步骤")
    do_process: bool = Field(True, description="执行格式化处理步骤")
    overwrite: bool = Field(False, description="强制重新获取远端元数据（覆盖缓存）")
    genome_date: Optional[str] = Field(None, description="用户手动设置的当前基因组更新日期（YYYY-MM-DD）")
    icon: Optional[str] = Field(None, description="自定义图标图片路径（服务器静态文件路径），如 /static/uploads/archaea.png；为空则按分类单元自动匹配")


class TaxonConfigResponse(BaseModel):
    id: int
    name: str
    taxon: str
    genome_type: str
    genome_dir: str
    tmp_dir: Optional[str]
    threads: int
    batch_size: int
    parallel_downloads: int
    check_interval_days:    int
    download_interval_days: int
    process_interval_days:  int
    last_auto_updated:      Optional[str]
    next_check_at:          Optional[str]
    next_download_at:       Optional[str]
    next_process_at:        Optional[str]
    pending_count:             int
    pending_format_count:      int = 0
    predownload_count:         int = 0
    predownload_interval_days: int = 0
    next_predownload_at:       Optional[str] = None
    do_check:    bool
    do_download: bool
    do_process:  bool
    overwrite:   bool
    genome_date: Optional[str] = None
    icon:        Optional[str] = None
    created_at: str
    updated_at: str


# ─────────────────────────────────────────────────────────────
# Genome 模型
# ─────────────────────────────────────────────────────────────

class GenomeResponse(BaseModel):
    id: int
    gca: str
    organism: Optional[str]
    length: Optional[int]
    taxid: Optional[str]
    lineage: Optional[str]
    genome_type: Optional[str]
    genome_dir: Optional[str]
    last_updated: str


# ─────────────────────────────────────────────────────────────
# 通用分页响应
# ─────────────────────────────────────────────────────────────

class PaginatedResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: list
