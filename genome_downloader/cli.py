#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
命令行入口模块。

负责：
- argparse 参数解析
- 调用各子模块按顺序执行完整下载流程
- 将子模块抛出的异常统一转换为 sys.exit 退出码

使用方式（直接运行包）::

    python -m genome_downloader --taxon fungi --genome_dir /data/fungi

或通过入口脚本::

    python mobilome_ncbi_genome_update.py --taxon fungi --genome_dir /data/fungi
"""
from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

from .deps import check_and_install_dependencies
from .downloader import download_genomes
from .exceptions import (
    CommandError,
    DependencyError,
    DownloadError,
    DownloaderError,
    NoUpdatesNeeded,
    ProcessingError,
)
from .logger import Logger
from .metadata import (
    check_updates_and_plan,
    convert_jsonl_to_tsv,
    fetch_genome_summary,
    parse_and_clean_metadata,
)
from .processor import build_blast_databases, validate_and_process_genomes, validate_blast_databases
from .repository import (
    rebuild_ref_links,
    _is_refseq_reference_category,
    rebuild_md5sums_from_genomes,
    update_metadata_table, update_repository,
)
from .utils import (
    GCA_PATTERN,
    non_ref_genomes_dir,
    ref_genomes_dir,
)
from .taxonomy import download_taxonomy_info


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """解析命令行参数。

    Args:
        argv: 参数列表，默认使用 sys.argv[1:]。
    """
    parser = argparse.ArgumentParser(
        description="NCBI Genome Batch Downloader & Updater",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--taxon", type=str, required=True,
        help="分类单元，如 fungi、bacteria",
    )
    parser.add_argument(
        "--genome_dir", type=str, required=True,
        help="基因组本地存储目录",
    )
    parser.add_argument(
        "--genome_type", default="ref", choices=["ref", "all"],
        help="基因组类型：参考(ref) 或 全部(all)",
    )
    parser.add_argument(
        "--overwrite", action="store_true",
        help="强制覆盖已有下载文件",
    )
    parser.add_argument(
        "--threads", type=int, default=4,
        help="并行线程数",
    )
    parser.add_argument(
        "--tmp_dir", type=str,
        help="临时工作目录（默认使用当前目录）",
    )
    parser.add_argument(
        "--batch_size", type=int, default=500,
        help="Taxonomy 下载每批最大 TaxID 数量",
    )
    parser.add_argument(
        "--parallel_downloads", type=int, default=4,
        help="Taxonomy 最大并行下载批次数",
    )
    parser.add_argument(
        "--api_key", type=str, default=None,
        help="NCBI API Key（可选，提升请求频率上限）",
    )
    parser.add_argument(
        "--skip_check", action="store_true",
        help="跳过检查+预下载步骤（使用上次生成的计划文件进行处理）",
    )
    parser.add_argument(
        "--skip_process", action="store_true",
        help="跳过格式化处理步骤（仅检查并下载到暂存区）",
    )
    # 内部标志：不显示在帮助中，供 Web API 调度「格式化基因组」步骤使用
    parser.add_argument(
        "--skip_download", action="store_true",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--validate_db", action="store_true",
        help="校验已建库的 BLAST 数据库完整性：对异常库先检查基因组 MD5，"
             "MD5 一致则重建数据库，MD5 不一致则报告需重新下载或处理基因组",
    )
    return parser.parse_args(argv)


def run_pipeline(
    taxon: str,
    genome_dir: str,
    genome_type: str = "ref",
    overwrite: bool = False,
    threads: int = 4,
    tmp_dir: str | None = None,
    batch_size: int = 500,
    parallel_downloads: int = 4,
    api_key: str | None = None,
    do_check: bool = True,
    do_download: bool = True,
    do_process: bool = True,
    do_validate_db: bool = False,
) -> None:
    """执行完整的下载/更新流水线。

    Args:
        do_check   : 是否执行检查+预下载步骤（获取远端元数据、下载到暂存区）
                     检查与预下载已合并为一个操作，do_check=True 时 do_download 自动置 True。
        do_download: 保留参数（向后兼容），当 do_check=True 时会被强制设为 True。
        do_process : 是否执行格式化处理步骤（校验、归档、元数据表）

    Raises:
        DependencyError : 缺少必要工具。
        CommandError    : 子命令执行失败。
        DownloadError   : 下载批次失败。
        ProcessingError : 处理阶段失败。
        NoUpdatesNeeded : 本地已是最新（非错误，调用方按需处理）。
    """
    genome_root  = Path(genome_dir).resolve()
    work_tmp_dir = Path(tmp_dir).resolve() if tmp_dir else (genome_root / "tmp")

    # 目录名与 taxon 不符时给出警告（交互式确认由 CLI 负责）
    if taxon.lower() not in genome_root.name.lower():
        Logger.warning(
            f"目标目录 '{genome_root.name}' 似乎不包含 taxon 名 '{taxon}'"
        )

    work_tmp_dir.mkdir(parents=True, exist_ok=True)
    genome_root.mkdir(parents=True, exist_ok=True)
    predownload_dir = work_tmp_dir / "predownload"

    # ── 检查与预下载合并：do_check=True 时强制带上 do_download ───────────────
    if do_check:
        do_download = True
    # 合并更新模式：do_check=False, do_download=True, do_process=False
    is_merge = not do_check and do_download and not do_process
    # ── 依赖检查 ──────────────────────────────────────────────────────────────
    check_and_install_dependencies()

    Logger.step(f"Task: Update {taxon} Genome (Type: {genome_type})")

    # ── Step 1: 元数据 ────────────────────────────────────────────────────────
    meta_json  = work_tmp_dir / "metadata.jsonl"
    meta_tsv   = work_tmp_dir / "metadata.tsv"
    meta_clean = work_tmp_dir / "metadata_clean.tsv"

    if do_check or is_merge:
        # 合并更新通常不会使用缓存覆盖元数据，但当有预下载/先前步骤已生成 metadata.jsonl 时
        # 我们在合并模式优先复用本地文件，避免再次向 NCBI 请求（特别是避免因 --reference 导致的条目丢失）
        meta_overwrite = True if is_merge else overwrite
        if is_merge and meta_json.exists():
            Logger.info("合并更新：检测到本地已有 metadata.jsonl，复用已存在文件，跳过重新请求 NCBI 元数据")
        else:
            fetch_genome_summary(taxon, genome_type, meta_json, meta_overwrite, api_key)
        convert_jsonl_to_tsv(meta_json, meta_tsv)
        count = parse_and_clean_metadata(meta_tsv, meta_clean)
        if count == 0:
            raise DownloaderError("未找到有效的基因组记录，流程中止。")

        # ── Step 1.3: 生成更新计划 ──────────────────────────────────────────────────────────
        # merge 模式不传 predownload_dir：让计划文件包含 predownload/ 中待合并的 GCA
        # （若传入 predownload_dir，这些 GCA 会被视为"已本地化"而从列表中剔除）
        try:
            list_file, taxid_file, deprecated_file = check_updates_and_plan(
                meta_clean, genome_root, predownload_dir if do_check else None
            )
        except NoUpdatesNeeded:
            if is_merge:
                Logger.info("合并更新：无新增下载，继续刷新元数据与 MD5。")
                list_file = None
                taxid_file = None
                deprecated_file = None
            else:
                raise

    else:
        # 格式化基因组模式（--skip_download）：使用上次生成的计划文件
        list_file        = work_tmp_dir / "download_list.txt"
        taxid_file       = work_tmp_dir / "taxid_list.txt"
        deprecated_file  = work_tmp_dir / "deprecated_list.txt"
        if list_file and (not list_file.exists() or list_file.stat().st_size == 0):
            list_file = None
        if taxid_file and (not taxid_file.exists() or taxid_file.stat().st_size == 0):
            taxid_file = None
        if deprecated_file and not deprecated_file.exists():
            deprecated_file = None

    if is_merge:
        # ── 合并更新：不执行任何序列下载，仅将预下载目录中的文件应用到 non_ref/genomes/ ───────
        Logger.step("Merging Updates")

        # 读取预下载 MD5 缓存
        md5_cache: dict[str, str] = {}
        md5_cache_file = predownload_dir / ".md5cache.tsv"
        if md5_cache_file.exists():
            for line in md5_cache_file.open():
                parts = line.strip().split("\t")
                if len(parts) == 2:
                    md5_cache[parts[0]] = parts[1]

        # 重新通过 datasets 拉取 Taxonomy，不复用旧缓存。
        if taxid_file and taxid_file.exists():
            Logger.info("合并更新：重新通过 datasets 获取 Taxonomy 信息")
            tax_report_dir = download_taxonomy_info(
                taxid_file,
                overwrite=True,
                batch_size=batch_size,
                api_key=api_key,
                parallel_downloads=parallel_downloads,
            )
        else:
            tax_report_dir = None

        # 从 predownload/ 移动可用文件，按 ref/non-ref 类型分别落盘
        ref_dest = ref_genomes_dir(genome_root)
        non_ref_dest = non_ref_genomes_dir(genome_root)
        moved_ref: list[str] = []
        moved_non_ref: list[str] = []
        skipped_gcas: list[str] = []
        type_map: dict[str, bool] = {}
        if meta_clean.exists():
            with meta_clean.open() as mf:
                for line in mf:
                    cols = line.strip().split("\t")
                    if not cols:
                        continue
                    gca = cols[0].strip()
                    refseq_category = cols[4].strip() if len(cols) > 4 else ""
                    is_ref = _is_refseq_reference_category(refseq_category)
                    type_map[gca] = is_ref
                    base = gca.rsplit(".", 1)[0] if "." in gca else gca
                    type_map.setdefault(base, is_ref)

        def _target_dir(gca: str) -> Path:
            is_ref = type_map.get(gca)
            if is_ref is None:
                base = gca.rsplit(".", 1)[0] if "." in gca else gca
                is_ref = type_map.get(base, False)
            return ref_dest if is_ref else non_ref_dest

        if list_file and list_file.exists():
            to_merge = [ln.strip() for ln in list_file.open() if ln.strip()]
            for gca in to_merge:
                pre_fna = predownload_dir / f"{gca}.fna"
                if not pre_fna.exists():
                    skipped_gcas.append(gca)
                    continue
                dest_dir = _target_dir(gca)
                dest_fna = dest_dir / f"{gca}.fna"
                alt_fna = (non_ref_dest if dest_dir == ref_dest else ref_dest) / f"{gca}.fna"
                if dest_fna.exists() or alt_fna.exists():
                    pre_fna.unlink(missing_ok=True)
                    continue
                dest_dir.mkdir(parents=True, exist_ok=True)
                shutil.move(str(pre_fna), str(dest_fna))
                if dest_dir == ref_dest:
                    moved_ref.append(gca)
                else:
                    moved_non_ref.append(gca)
        moved_total = len(moved_ref) + len(moved_non_ref)
        if moved_total:
            Logger.info(
                f"已从预下载目录移动 {moved_total} 个基因组（ref={len(moved_ref)}, non-ref={len(moved_non_ref)}）"
            )
        if skipped_gcas:
            preview = ', '.join(skipped_gcas[:5]) + ('...' if len(skipped_gcas) > 5 else '')
            Logger.warning(f"{len(skipped_gcas)} 个基因组在预下载目录中不存在，已跳过: {preview}")

        # 更新元数据表
        if meta_clean.exists():
            Logger.step("Updating Metadata")
            update_metadata_table(meta_clean, tax_report_dir, genome_root, genome_type)

        # 处理废弃基因组 + 重建 ref 链接
        Logger.step("Updating Repository")
        update_repository(None, genome_root, deprecated_file)
        rebuild_ref_links(genome_root)
        Logger.step("Refreshing MD5")
        rebuild_md5sums_from_genomes(
            genome_root,
            md5_cache,
            work_tmp_dir=work_tmp_dir,
            batch_size=batch_size,
            api_key=api_key,
            parallel_downloads=parallel_downloads,
        )
        Logger.step("ALL DONE!", level="SUCCESS")
        return

    elif do_download and list_file:
        # ── Step 2: 下载 ──────────────────────────────────────────────────────
        Logger.step("Downloading Data")
        # 预下载 = do_check + do_download（无 do_process）：仅暂存到 staging，跳过 taxonomy/metadata
        # 合并基因组 = do_download（无 do_check）或完整流程：移入 non_ref/genomes/，执行 taxonomy+metadata
        is_predownload = do_check and not do_process
        stage_only = is_predownload

        if do_check:
            # 检查+预下载 或 完整流程：直接按当前 TaxID 列表重新下载 Taxonomy
            tax_report_dir = download_taxonomy_info(
                taxid_file, overwrite=True, batch_size=batch_size,
                api_key=api_key, parallel_downloads=parallel_downloads,
            )
        else:
            # do_check=False 时，仍然按当前 TaxID 列表重新下载 Taxonomy，避免复用旧缓存
            if taxid_file and taxid_file.exists():
                tax_report_dir = download_taxonomy_info(
                    taxid_file, overwrite=True, batch_size=batch_size,
                    api_key=api_key, parallel_downloads=parallel_downloads,
                )
            else:
                tax_report_dir = None

        raw_data_dir = download_genomes(
            list_file, genome_root, overwrite, api_key,
            stage_only=stage_only,
            predownload_dir=predownload_dir,
            batch_size=batch_size,
            parallel_downloads=parallel_downloads,
        )

        # 元数据表更新：仅在合并/完整流程（文件已进入 non_ref/genomes/）时执行
        if not is_predownload and meta_clean.exists():
            Logger.step("Updating Metadata")
            update_metadata_table(meta_clean, tax_report_dir, genome_root, genome_type)

    elif not do_download:
        # 格式化基因组模式（--skip_check --skip_download）：
        # do_process 将直接扫描 genome_root 下两个最终目录，不需要 raw_data_dir
        raw_data_dir   = None
        tax_report_dir = None
    else:
        # list_file 为空（无待更新基因组），无需下载
        Logger.info("[SKIP DOWNLOAD] 无待下载基因组，跳过下载步骤。")
        tax_report_dir = None
        raw_data_dir   = None

    if do_process:
        # ── Step 3: 删除废弃基因组 ────────────────────────────────────────────
        Logger.step("Updating Repository")
        update_repository(None, genome_root, deprecated_file)

        # 只有当新文件已写入 non_ref/genomes/（完整流程或合并更新后紧接进行格式化）时才调用
        # 格式化基因组（--skip_check --skip_download）模式下 ref 文件已在正确位置，无需重分拣
        if do_download:
            rebuild_ref_links(genome_root)

        # ── Step 4: BLAST 数据库构建 ────────────────────────────────
        # 扫描 non_ref/genomes/ 和 ref/genomes/，
        # 按文件目前实际位置检查库完整性（.nhr + .fai），仅对不完整的建库
        Logger.step("Processing Genomes")
        total, built, skipped = build_blast_databases(genome_root, threads)
        if built > 0:
            Logger.info(f"建库完成：{built}/{total} 个基因组，跳过已完整 {skipped} 个。")

        # ── Step 5: 校验 BLAST 数据库 ────────────────────────────────────────
        if do_validate_db:
            Logger.step("Validating BLAST Databases")
            _, _, _, repair_needed = validate_blast_databases(genome_root, threads)
            if repair_needed:
                Logger.warning(
                    f"{len(repair_needed)} 个基因组文件 MD5 不匹配，请重新下载或处理。"
                )

        Logger.step("ALL DONE!", level="SUCCESS")

    elif do_validate_db and not do_process:
        # 仅校验数据库（不重建）
        Logger.step("Validating BLAST Databases")
        _, _, _, repair_needed = validate_blast_databases(genome_root, threads)
        if repair_needed:
            Logger.warning(
                f"{len(repair_needed)} 个基因组文件 MD5 不匹配，请重新下载或处理。"
            )
        Logger.step("ALL DONE!", level="SUCCESS")

    elif do_download and not do_process and raw_data_dir:
        if is_predownload:
            # 预下载完成：文件已暂存到 predownload/，等待合并更新步骤
            Logger.step("ALL DONE!", level="SUCCESS")
        else:
            # 无 BLAST 构建（do_download but do_process=False, not is_merge）
            Logger.step("Updating Repository")
            update_repository(None, genome_root, deprecated_file)
            rebuild_ref_links(genome_root)
            Logger.step("ALL DONE!", level="SUCCESS")

    elif deprecated_file and deprecated_file.exists():
        # 仅有废弃基因组，无新文件需处理
        update_repository(None, genome_root, deprecated_file)
        rebuild_ref_links(genome_root)
        Logger.step("ALL DONE!", level="SUCCESS")

    else:
        Logger.success("No updates required.")


def main(argv: list[str] | None = None) -> None:
    """命令行入口：解析参数 → 交互确认 → 执行流水线 → 处理异常。"""
    args = parse_args(argv)

    # 目录名与 taxon 不符时交互确认
    genome_root = Path(args.genome_dir).resolve()
    if args.taxon.lower() not in genome_root.name.lower():
        Logger.warning(
            f"目标目录 '{genome_root.name}' 似乎不包含 taxon 名 '{args.taxon}'"
        )
        try:
            answer = input("Continue? [y/N] ")
        except (EOFError, KeyboardInterrupt):
            answer = ""
        if answer.strip().lower() != "y":
            sys.exit(0)

    try:
        run_pipeline(
            taxon              = args.taxon,
            genome_dir         = args.genome_dir,
            genome_type        = args.genome_type,
            overwrite          = args.overwrite,
            threads            = args.threads,
            tmp_dir            = args.tmp_dir,
            batch_size         = args.batch_size,
            parallel_downloads = args.parallel_downloads,
            api_key            = args.api_key,
            do_check           = not args.skip_check,
            do_download        = not args.skip_download,
            do_process         = not args.skip_process,
            do_validate_db     = args.validate_db,
        )
    except NoUpdatesNeeded as e:
        Logger.success(str(e))
        sys.exit(0)
    except DependencyError as e:
        Logger.error(f"[DependencyError] {e}")
        sys.exit(1)
    except CommandError as e:
        sys.exit(e.returncode)
    except (DownloadError, ProcessingError, DownloaderError) as e:
        Logger.error(f"[{type(e).__name__}] {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        Logger.warning("用户中断（Ctrl+C）")
        sys.exit(130)
