#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Taxonomy 下载模块 — Step 2.1。

支持将大量 TaxID 自动拆分为多批次并行下载，
最后合并为统一的 taxonomy_summary.tsv。

Raises:
    DownloadError: 任意批次经过最大重试次数后仍然失败。
"""
from __future__ import annotations

import shutil
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from .exceptions import DownloadError
from .logger import Logger

# taxonomy_summary.tsv 文件有效期（天）。超过此期限将强制全量重新下载。
TAXONOMY_TTL_DAYS: int = 7


def _download_one_taxonomy_batch(
    idx: int,
    total_batches: int,
    batch: list[str],
    parent_dir: Path,
    overwrite: bool,
    api_key: str | None,
    max_retries: int = 3,
) -> tuple[int, Path | None]:
    """下载单个 Taxonomy 批次（供 ThreadPoolExecutor 调用）。

    失败时自动递增等待重试，最多 max_retries 次。

    Returns:
        (idx, batch_summary_path or None)
    """
    batch_file = parent_dir / f"taxid_batch_{idx}.txt"
    batch_zip  = parent_dir / f"taxid_batch_{idx}.zip"
    batch_dir  = parent_dir / f"taxonomy_batch_{idx}"

    with batch_file.open("w") as f:
        f.writelines(f"{tid}\n" for tid in batch)

    Logger.info(f"[批次 {idx}/{total_batches}] 包含 {len(batch)} 个 TaxID")

    batch_summary = batch_dir / "ncbi_dataset" / "data" / "taxonomy_summary.tsv"

    if batch_summary.exists():
        Logger.success(f"  批次 {idx} 已存在，直接复用。")
        return idx, batch_summary

    if batch_zip.exists():
        Logger.info(f"  批次 {idx} 压缩包已存在，尝试复用。")
        batch_dir.mkdir(parents=True, exist_ok=True)
        subprocess.run(
            f"unzip -o {batch_zip} -d {batch_dir}",
            shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        if batch_summary.exists():
            Logger.success(f"  批次 {idx} 完成。")
            return idx, batch_summary
        Logger.warning(f"  批次 {idx} 现有压缩包未产出有效 taxonomy_summary.tsv，准备重新下载。")
        shutil.rmtree(batch_dir, ignore_errors=True)
        batch_zip.unlink(missing_ok=True)

    api_key_opt = f" --api-key {api_key}" if api_key else ""
    cmd = (
        f"datasets download taxonomy taxon "
        f"--inputfile {batch_file} --filename {batch_zip}{api_key_opt}"
    )
    for attempt in range(1, max_retries + 1):
        Logger.shell(f"[尝试 {attempt}/{max_retries}] {cmd}")
        if batch_zip.exists():
            batch_zip.unlink()   # 删除可能不完整的残留文件
        proc = subprocess.run(
            cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        if proc.returncode == 0 and batch_zip.exists():
            break
        Logger.warning(
            f"  批次 {idx} 第 {attempt} 次尝试失败 (returncode={proc.returncode})"
        )
        if attempt < max_retries:
            wait = 10 * attempt   # 递增等待：10s, 20s, 30s
            Logger.info(f"  等待 {wait}s 后重试...")
            time.sleep(wait)

    if not batch_zip.exists():
        Logger.error(f"  批次 {idx} 经过 {max_retries} 次重试后仍然失败！")
        return idx, None

    # 解压
    batch_dir.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        f"unzip -o {batch_zip} -d {batch_dir}",
        shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    if batch_summary.exists():
        Logger.success(f"  批次 {idx} 完成。")
        return idx, batch_summary
    else:
        Logger.error(f"  批次 {idx} 解压后未找到 taxonomy_summary.tsv！")
        return idx, None


def download_taxonomy_info(
    taxid_list_file: Path | None,
    overwrite: bool = False,
    batch_size: int = 500,
    api_key: str | None = None,
    parallel_downloads: int = 4,
) -> Path | None:
    """Step 2.1: 批量下载 Taxonomy 信息并合并为统一 TSV。

    当 TaxID 数量超过 batch_size 时，自动拆分为多批次并行下载，
    完成后合并所有批次的 taxonomy_summary.tsv，并清理临时文件。

    Args:
        taxid_list_file:    TaxID 列表文件路径（None 或不存在时直接返回 None）。
        overwrite:          是否强制重新下载已有批次。
        batch_size:         每批最大 TaxID 数量。
        api_key:            NCBI API Key（可选）。
        parallel_downloads: 最大并行批次数。

    Returns:
        合并后 taxonomy_report 目录路径；无需下载时返回 None。

    Raises:
        DownloadError: 任意批次下载失败。
    """
    if not taxid_list_file or not taxid_list_file.exists():
        return None

    work_root       = taxid_list_file.parent
    batch_root      = work_root / "batch_taxonomy"
    report_dir      = work_root / "taxonomy"
    report_dir.mkdir(parents=True, exist_ok=True)
    merged_data_dir = report_dir / "ncbi_dataset" / "data"
    merged_data_dir.mkdir(parents=True, exist_ok=True)
    merged_summary  = merged_data_dir / "taxonomy_summary.tsv"

    with taxid_list_file.open("r") as f:
        all_taxids = [line.strip() for line in f if line.strip()]

    if not all_taxids:
        Logger.warning("TaxID 列表为空，跳过 Taxonomy 下载。")
        return None

    # 记录 NCBI 始终不返回数据的 TaxID（避免每次全量重下载）
    unavailable_file = merged_data_dir / "unavailable_taxids.txt"
    unavailable_taxids: set[str] = set()
    if unavailable_file.exists():
        unavailable_taxids = set(unavailable_file.read_text().strip().splitlines())

    # 记录上次合并时的 TaxID 总数，用于判断缺失 TaxID 是"新增"还是"永久缺失"
    merge_info_file = merged_data_dir / ".merge_info"
    merge_total: int | None = None
    if merge_info_file.exists():
        try:
            merge_total = int(merge_info_file.read_text().strip())
        except (ValueError, OSError):
            pass

    if merged_summary.exists():
        file_age_days = (time.time() - merged_summary.stat().st_mtime) / 86400
        if file_age_days > TAXONOMY_TTL_DAYS:
            Logger.info(
                f"Taxonomy 合并文件已过期（{file_age_days:.1f} 天，超过 {TAXONOMY_TTL_DAYS} 天），"
                f"将清除缓存标记并重新全量下载。"
            )
            # 清理旧标记，使下方逻辑跳过复用分支，执行全量下载
            merge_info_file.unlink(missing_ok=True)
            unavailable_file.unlink(missing_ok=True)
        else:
            Logger.info(
                f"Taxonomy 合并文件在有效期内（{file_age_days:.1f} 天 ≤ {TAXONOMY_TTL_DAYS} 天），"
                f"复用缓存并仅补齐缺失 TaxID。"
            )
            cached_taxids: set[str] = set()
            with merged_summary.open("r") as f:
                f.readline()  # 跳过表头
                for line in f:
                    cols = line.strip().split("\t")
                    if len(cols) > 1 and cols[1]:
                        cached_taxids.add(cols[1])

            missing_taxids = [tid for tid in all_taxids if tid not in cached_taxids]
            # 排除已知的永久缺失 TaxID
            genuine_missing = [tid for tid in missing_taxids if tid not in unavailable_taxids]

            # 如果 .merge_info 不存在但合并文件存在（旧版本代码遗留），
            # 以当前 TaxID 总数创建标记，并将缺失 TaxID 视为永久不可用
            if genuine_missing and merge_total is None:
                Logger.info(
                    f"Taxonomy 合并文件已存在但缺少合并记录，"
                    f"将当前 {len(missing_taxids)} 个缺失 TaxID 标记为不可用。"
                )
                merge_info_file.write_text(str(len(all_taxids)) + "\n")
                unavailable_file.write_text("\n".join(sorted(genuine_missing)) + "\n")
                return report_dir

            # 如果 TaxID 总数未变且存在缺失，说明缺失的是 NCBI 永久无数据的记录
            if genuine_missing and merge_total == len(all_taxids):
                Logger.info(
                    f"Taxonomy 合并文件缺少 {len(genuine_missing)} 个 TaxID，"
                    f"但 TaxID 总数未变（{merge_total}），标记为不可用并跳过下载。"
                )
                all_unavailable = sorted(set(unavailable_taxids) | set(genuine_missing))
                unavailable_file.write_text("\n".join(all_unavailable) + "\n")
                return report_dir

            if not genuine_missing:
                if missing_taxids:
                    Logger.info(
                        f"Taxonomy 合并文件缺少 {len(missing_taxids)} 个 TaxID，"
                        f"均为 NCBI 无数据记录（已标记为不可用），跳过下载。"
                    )
                else:
                    Logger.info("Taxonomy 合并文件已包含当前全部 TaxID，直接复用。")
                return report_dir

            Logger.info(
                f"Taxonomy 合并文件缺少 {len(genuine_missing)} 个当前 TaxID"
                f"（另有 {len(missing_taxids) - len(genuine_missing)} 个已标记为不可用），"
                f"将复用已完成批次并继续补齐。"
            )

    total = len(all_taxids)
    Logger.info(f"共有 {total} 个 TaxID 需要下载 Taxonomy 信息 (batch_size={batch_size})")

    batch_root.mkdir(parents=True, exist_ok=True)

    # 分批并行下载
    batches     = [all_taxids[i:i + batch_size] for i in range(0, total, batch_size)]
    num_batches = len(batches)
    workers     = min(parallel_downloads, num_batches)
    Logger.info(f"将分 {num_batches} 批次下载 Taxonomy 数据 (并行数: {workers})...")

    batch_results: dict[int, Path | None] = {}

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                _download_one_taxonomy_batch,
                idx, num_batches, batch, batch_root, overwrite, api_key,
            ): idx
            for idx, batch in enumerate(batches, start=1)
        }
        for future in as_completed(futures):
            idx, summary_path = future.result()
            batch_results[idx] = summary_path

    # 检查失败批次
    failed = sorted(idx for idx, p in batch_results.items() if p is None)
    if failed:
        raise DownloadError(
            f"以下 Taxonomy 批次下载失败，数据不完整: {failed}"
        )

    # 按序合并
    header_line: str | None = None
    all_data_lines: list[str] = []
    for idx in sorted(batch_results.keys()):
        summary_path = batch_results[idx]
        if summary_path and summary_path.exists():
            with summary_path.open("r") as f:
                first = f.readline()
                if header_line is None:
                    header_line = first
                all_data_lines.extend(line for line in f if line.strip())

    if header_line:
        with merged_summary.open("w") as f:
            f.write(header_line)
            f.writelines(all_data_lines)
        Logger.success(
            f"Taxonomy 合并完成: {merged_summary} ({len(all_data_lines)} 条记录)"
        )

        # 记录 NCBI 始终无数据的 TaxID，避免后续每次全量重下载
        merged_taxids: set[str] = set()
        for line in all_data_lines:
            cols = line.strip().split("\t")
            if len(cols) > 1 and cols[1]:
                merged_taxids.add(cols[1])
        still_missing = sorted(tid for tid in all_taxids if tid not in merged_taxids)
        if still_missing:
            unavailable_file.write_text("\n".join(still_missing) + "\n")
            Logger.warning(
                f"以下 {len(still_missing)} 个 TaxID 在 NCBI 中无 Taxonomy 数据，"
                f"已标记为不可用: {', '.join(still_missing[:10])}"
                f"{' ...' if len(still_missing) > 10 else ''}"
            )
        elif unavailable_file.exists():
            unavailable_file.unlink()  # 所有 TaxID 均已覆盖，清理旧标记

        # 记录本次合并的 TaxID 总数，供后续判断是否有新增 TaxID
        merge_info_file.write_text(str(len(all_taxids)) + "\n")
    else:
        Logger.warning("所有批次均未产生有效的 taxonomy_summary.tsv")

    # 清理临时文件
    for idx in range(1, num_batches + 1):
        for name in (f"taxid_batch_{idx}.txt", f"taxid_batch_{idx}.zip"):
            tmp = batch_root / name
            if tmp.exists():
                tmp.unlink()
        tmp_dir = batch_root / f"taxonomy_batch_{idx}"
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir)

    if batch_root.exists():
        try:
            batch_root.rmdir()
        except OSError:
            pass

    return report_dir
