#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
genome_downloader — NCBI 基因组批量下载核心包。

从 mobilome_ncbi_genome_update.py 解耦重构而来，
各子模块职责单一，可被 genome_manager (Web 后端) 直接 import。

子模块速览
----------
exceptions  : 自定义异常类（替代 sys.exit，便于上层调用方捕获）
logger      : 终端彩色日志 Logger
utils       : GCA_PATTERN / run_cmd / run_shell / check_gz_integrity
deps        : 依赖检查与自动安装（datasets / dataformat 等 NCBI 工具）
metadata    : Step 1 — 元数据获取、解析、更新计划
taxonomy    : Step 2.1 — Taxonomy 分批并行下载
downloader  : Step 2.2 — 基因组 Dehydrated 下载 + Rehydrate
processor   : Step 3&4 — 解压 / MD5 校验 / BLAST 数据库构建
repository  : Step 5&6 — 文件归档 / 索引重建 / 元数据表更新
cli         : argparse 解析 + main() 命令行入口
"""
from .exceptions import (
    DownloaderError,
    DependencyError,
    CommandError,
    DownloadError,
    ProcessingError,
    NoUpdatesNeeded,
)
from .logger import Logger
from .metadata import (
    fetch_genome_summary,
    convert_jsonl_to_tsv,
    parse_and_clean_metadata,
    check_updates_and_plan,
)
from .taxonomy import download_taxonomy_info
from .downloader import download_genomes
from .processor import validate_and_process_genomes
from .repository import rebuild_ref_links, update_repository, update_metadata_table, compute_md5sums
from .cli import main

__all__ = [
    # exceptions
    "DownloaderError", "DependencyError", "CommandError",
    "DownloadError", "ProcessingError", "NoUpdatesNeeded",
    # logger
    "Logger",
    # pipeline
    "fetch_genome_summary", "convert_jsonl_to_tsv",
    "parse_and_clean_metadata", "check_updates_and_plan",
    "download_taxonomy_info", "download_genomes",
    "validate_and_process_genomes",
    "rebuild_ref_links", "update_repository", "update_metadata_table",
    "compute_md5sums",
    # entry point
    "main",
]
