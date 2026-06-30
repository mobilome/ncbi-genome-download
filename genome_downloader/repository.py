#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
仓库管理模块 — Step 5 & 6。

目录结构
--------
genome_dir/
    non_ref/
        genomes/                        ← 非 ref 类型基因组（实体文件）
        md5sums.txt                     ← 非 ref 基因组 MD5
        genomes_metadata.tsv            ← 元数据缓存（rebuild_ref_links 后仅含非 ref 条目）
    ref/
        genomes/                        ← ref 类型基因组（实体文件）
        md5sums.txt                     ← ref 基因组 MD5
        genomes_metadata.tsv            ← ref 基因组元数据

函数列表
--------
update_repository()     : 将处理好的文件归档到 non_ref/genomes/，删除已废弃文件（同时检查 ref/genomes/）
rebuild_ref_links()     : 将 ref 类型文件从 non_ref/genomes/ 移入 ref/genomes/，生成同级 md5sums.txt 和 genomes_metadata.tsv
update_metadata_table() : 合并 Taxonomy 信息，生成 non_ref/genomes_metadata.tsv（含全量条目，供后续分拣）
rebuild_md5sums_from_genomes() : 基于当前 .fna 列表重建 ref/non_ref 的 md5sums.txt
"""
from __future__ import annotations

import os
import shutil
import subprocess
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from .exceptions import DownloadError
from .logger import Logger
from .utils import (
    GCA_PATTERN,
    check_gz_integrity,
    non_ref_genomes_dir,
    non_ref_md5_path,
    non_ref_metadata_path,
    parse_ncbi_md5sum,
    ref_genomes_dir,
    ref_md5_path,
    ref_metadata_path,
    run_cmd,
)


def _is_refseq_reference_category(refseq_category: str) -> bool:
    """判断 NCBI RefSeq 分类是否属于参考基因组。"""
    normalized = refseq_category.strip().lower()
    return normalized in {"ref", "reference genome"}


def compute_md5sums(genome_dir: Path) -> dict[str, str]:
    """计算 non_ref/genomes/ 和 ref/genomes/ 中 .fna 的真实 MD5，分别写入对应 md5sums 文件并返回合并映射。

    已有正确长度（32 位十六进制）MD5 记录的文件跳过重算，节省时间。

    Returns:
        {gca: md5hex} 合并字典（含两个目录的全量记录）。
    """
    import hashlib

    result: dict[str, str] = {}
    total_computed = 0

    for genomes_dir, md5sums_file in [
        (non_ref_genomes_dir(genome_dir), non_ref_md5_path(genome_dir)),
        (ref_genomes_dir(genome_dir),     ref_md5_path(genome_dir)),
    ]:

        existing: dict[str, str] = {}
        if md5sums_file.exists():
            for line in md5sums_file.open():
                parts = line.strip().split()
                if len(parts) == 2 and len(parts[1]) == 32:
                    existing[parts[0]] = parts[1]

        sub_result: dict[str, str] = dict(existing)
        computed = 0

        if genomes_dir.exists():
            for fna in sorted(genomes_dir.glob("*.fna")):
                m = GCA_PATTERN.search(fna.name)
                if not m:
                    continue
                gca = m.group(1)
                if gca in existing:
                    continue
                Logger.info(f" 计算 MD5: {fna.name}")
                hasher = hashlib.md5()
                with fna.open("rb") as f:
                    while chunk := f.read(1 << 20):
                        hasher.update(chunk)
                sub_result[gca] = hasher.hexdigest()
                computed += 1

        md5sums_file.parent.mkdir(parents=True, exist_ok=True)
        with md5sums_file.open("w") as f:
            for gca in sorted(sub_result):
                f.write(f"{gca}\t{sub_result[gca]}\n")

        result.update(sub_result)
        total_computed += computed

    Logger.success(f"MD5 计算完成：新计算 {total_computed} 个，共 {len(result)} 条记录")
    return result



def _download_one_md5_batch(
    idx: int,
    total_batches: int,
    batch: list[str],
    parent_dir: Path,
    api_key: str | None,
    max_retries: int = 3,
) -> tuple[int, dict[str, str] | None]:
    """下载单个 MD5 批次并解析 md5sum.txt。"""
    batch_dir = parent_dir / f"batch_{idx:04d}"
    batch_dir.mkdir(parents=True, exist_ok=True)

    batch_file = batch_dir / "md5_accessions.txt"
    batch_zip = batch_dir / "md5_dataset.zip"
    unzip_dir = batch_dir / "md5_dataset"

    with batch_file.open("w") as f:
        f.writelines(f"{gca}\n" for gca in batch)

    Logger.info(f"[批次 {idx}/{total_batches}] 包含 {len(batch)} 个 GCA")

    api_key_opt = f" --api-key {api_key}" if api_key else ""
    cmd = (
        f"datasets download genome accession --inputfile {batch_file} "
        f"--dehydrated --filename {batch_zip}{api_key_opt}"
    )
    for attempt in range(1, max_retries + 1):
        Logger.shell(f"[尝试 {attempt}/{max_retries}] {cmd}")
        if batch_zip.exists():
            batch_zip.unlink()
        proc = subprocess.run(
            cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        if proc.returncode == 0 and batch_zip.exists():
            break
        Logger.warning(
            f" 批次 {idx} 第 {attempt} 次尝试失败 (returncode={proc.returncode})"
        )
        if attempt < max_retries:
            wait = 10 * attempt
            Logger.info(f" 等待 {wait}s 后重试...")
            time.sleep(wait)

    if not batch_zip.exists():
        Logger.error(f" 批次 {idx} 经过 {max_retries} 次重试后仍然失败！")
        return idx, None

    if unzip_dir.exists():
        shutil.rmtree(unzip_dir)
    unzip_dir.mkdir(parents=True, exist_ok=True)
    run_cmd(f"unzip -o {batch_zip} -d {unzip_dir}", verbose=False)

    md5_map: dict[str, str] = {}
    for cand in [
        unzip_dir / "md5sum.txt",
        unzip_dir / "ncbi_dataset" / "md5sum.txt",
        unzip_dir / "ncbi_dataset" / "data" / "md5sum.txt",
    ]:
        md5_map.update(parse_ncbi_md5sum(cand))

    if md5_map:
        Logger.success(f" 批次 {idx} 完成。")
        return idx, md5_map

    Logger.error(f" 批次 {idx} 未找到有效 md5sum.txt！")
    return idx, None


def rebuild_md5sums_from_genomes(
    genome_dir: Path,
    md5_cache: dict[str, str] | None = None,
    work_tmp_dir: Path | None = None,
    batch_size: int = 500,
    api_key: str | None = None,
    parallel_downloads: int = 4,
    max_retries: int = 3,
) -> None:
    """基于当前 .fna 列表重建 ref/non_ref 的 md5sums.txt。

    优先使用 datasets 提供的 md5_cache 与已有 md5sums 中的 32 位 MD5；
    对缺失项按批次调用 datasets 获取 MD5（不做本地计算）。
    """
    md5_cache = md5_cache or {}
    work_tmp_dir = work_tmp_dir or (genome_dir / "tmp")

    def _scan_gcas(genomes_dir: Path) -> list[str]:
        gcas: list[str] = []
        if genomes_dir.exists():
            for fna in sorted(genomes_dir.glob("*.fna")):
                m = GCA_PATTERN.search(fna.name)
                if m:
                    gcas.append(m.group(1))
        return gcas

    def _load_existing(path: Path) -> dict[str, str]:
        existing: dict[str, str] = {}
        if path.exists():
            for line in path.open():
                parts = line.strip().split()
                if len(parts) == 2 and len(parts[1]) == 32:
                    existing[parts[0]] = parts[1]
        return existing

    non_ref_gcas = _scan_gcas(non_ref_genomes_dir(genome_dir))
    ref_gcas = _scan_gcas(ref_genomes_dir(genome_dir))
    all_gcas = sorted(set(non_ref_gcas + ref_gcas))

    existing_all = _load_existing(non_ref_md5_path(genome_dir))
    existing_all.update(_load_existing(ref_md5_path(genome_dir)))

    md5_values: dict[str, str] = {}
    missing: list[str] = []
    for gca in all_gcas:
        if gca in md5_cache:
            md5_values[gca] = md5_cache[gca]
        elif gca in existing_all:
            md5_values[gca] = existing_all[gca]
        else:
            missing.append(gca)

    if missing:
        effective_batch = max(1, batch_size)
        total = len(missing)
        Logger.info(
            f"共有 {total} 个 GCA 需要下载 MD5 信息 (batch_size={effective_batch})"
        )
        batches = [missing[i:i + effective_batch] for i in range(0, total, effective_batch)]
        num_batches = len(batches)
        workers = min(max(1, parallel_downloads), num_batches)
        Logger.info(
            f"将分 {num_batches} 批次下载 MD5 数据 (并行数: {workers})..."
        )

        md5_root = work_tmp_dir / "md5_batches"
        md5_root.mkdir(parents=True, exist_ok=True)
        batch_results: dict[int, dict[str, str] | None] = {}

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(
                    _download_one_md5_batch,
                    idx,
                    num_batches,
                    batch,
                    md5_root,
                    api_key,
                    max_retries,
                ): idx
                for idx, batch in enumerate(batches, start=1)
            }
            for future in as_completed(futures):
                idx, md5_map = future.result()
                batch_results[idx] = md5_map

        failed = sorted(idx for idx, m in batch_results.items() if m is None)
        if failed:
            Logger.warning(f"以下 MD5 批次下载失败，已保留其他批次结果: {failed}")

        fetched: dict[str, str] = {}
        for idx in sorted(batch_results.keys()):
            md5_map = batch_results[idx]
            if md5_map:
                fetched.update(md5_map)

        if fetched:
            Logger.success(f"MD5 合并完成 ({len(fetched)} 条记录)")

        for gca, md5 in fetched.items():
            if md5:
                md5_values[gca] = md5

        still_missing = [gca for gca in missing if gca not in md5_values]

        for idx in range(1, num_batches + 1):
            batch_dir = md5_root / f"batch_{idx:04d}"
            if batch_dir.exists():
                shutil.rmtree(batch_dir, ignore_errors=True)

        if still_missing:
            preview = ", ".join(still_missing[:5]) + ("..." if len(still_missing) > 5 else "")
            Logger.warning(
                f"{len(still_missing)} 个基因组未获取到 MD5，已写入其余 {len(md5_values)} 条记录，示例: {preview}"
            )

    def _write_md5sums(path: Path, gcas: list[str], label: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w") as f:
            written = 0
            for gca in sorted(gcas):
                md5 = md5_values.get(gca)
                if not md5:
                    continue
                f.write(f"{gca}\t{md5}\n")
                written += 1
        Logger.info(f"{label}/md5sums.txt 已刷新（共 {written} 条记录）")

    _write_md5sums(non_ref_md5_path(genome_dir), non_ref_gcas, "non_ref")
    _write_md5sums(ref_md5_path(genome_dir), ref_gcas, "ref")


def rebuild_ref_links(genome_dir: Path) -> int:
    """将 ref 类型基因组从 non_ref/genomes/ 移入 ref/genomes/（实体文件），生成同级 md5sums.txt 和 genomes_metadata.tsv。

    目录结构（调用后）：
        genome_dir/non_ref/genomes/         ← 非 ref 类型基因组（实体文件）
        genome_dir/ref/genomes/             ← ref 类型基因组（实体文件，从 non_ref/genomes/ 移入）
        genome_dir/ref/md5sums.txt          ← ref 基因组 MD5
        genome_dir/ref/genomes_metadata.tsv ← ref 基因组元数据

    Returns:
        移动到 ref/genomes/ 的文件数量。
    """
    non_ref_dir = non_ref_genomes_dir(genome_dir)
    ref_dir     = ref_genomes_dir(genome_dir)
    meta_file   = non_ref_metadata_path(genome_dir)
    non_ref_md5 = non_ref_md5_path(genome_dir)
    ref_md5     = ref_md5_path(genome_dir)

    def _progress(label: str, current: int, total: int) -> None:
        if total <= 0:
            return
        if current == total or current % 100000 == 0:
            Logger.info(f"[ref links] {label} {current}/{total}")

    def _tick(label: str, current: int, interval: int = 100000) -> None:
        if current % interval == 0:
            Logger.info(f"[ref links] {label} {current}")

    # ── 1. 从 non_ref/genomes_metadata.tsv 读取 ref 类型 GCA ─────────────────
    ref_gcas: set[str] = set()
    if not meta_file.exists():
        Logger.warning("未找到 non_ref/genomes_metadata.tsv，跳过 ref/genomes/ 构建。")
        return 0

    with meta_file.open() as f:
        header_line = f.readline()
        meta_count = 0
        for line in f:
            meta_count += 1
            cols = line.strip().split("\t")
            if len(cols) >= 6 and cols[5].strip().lower() == "ref":
                ref_gcas.add(cols[0].strip())
            _tick("读取 non_ref/genomes_metadata.tsv", meta_count)
        Logger.info(f"[ref links] 读取 non_ref/genomes_metadata.tsv 完成 {meta_count}")
    # 仅在元数据为空时才回退读取 ref/genomes_metadata.tsv，
    # 避免历史误分类把 non-ref GCA 永久“粘”在 ref 集合中。
    ref_meta_existing = ref_metadata_path(genome_dir)
    if meta_count == 0 and ref_meta_existing.exists():
        with ref_meta_existing.open() as f:
            f.readline()  # 跳过表头
            ref_meta_count = 0
            for line in f:
                ref_meta_count += 1
                cols = line.strip().split("\t")
                if cols and cols[0]:
                    ref_gcas.add(cols[0].strip())
                _tick("读取 ref/genomes_metadata.tsv", ref_meta_count)
            Logger.info(f"[ref links] 读取 ref/genomes_metadata.tsv 完成 {ref_meta_count}")
    # ── 2. 确保 ref/genomes/ 目录存在 ──────────────────────────────────────
    ref_dir.mkdir(parents=True, exist_ok=True)

    # ── 3. 将 ref GCA 从 non_ref/genomes/ 移入 ref/genomes/ ────────────────
    Logger.info("扫描 ref/non_ref 目录文件列表...")
    ref_existing = {
        entry.name[:-4]
        for entry in os.scandir(ref_dir)
        if entry.is_file() and entry.name.endswith(".fna")
    }
    non_ref_existing = {
        entry.name[:-4]
        for entry in os.scandir(non_ref_dir)
        if entry.is_file() and entry.name.endswith(".fna")
    } if non_ref_dir.exists() else set()

    moved = 0
    missing_count = 0
    missing_samples: list[str] = []
    ref_gca_list = sorted(ref_gcas)
    total_ref_gcas = len(ref_gca_list)
    for idx, gca in enumerate(ref_gca_list, start=1):
        in_ref = gca in ref_existing
        in_non_ref = gca in non_ref_existing

        if in_ref:
            # 已在 ref/genomes/，若 non_ref/genomes/ 还有残留则删除
            if in_non_ref:
                (non_ref_dir / f"{gca}.fna").unlink(missing_ok=True)
                non_ref_existing.discard(gca)
        elif in_non_ref:
            shutil.move(str(non_ref_dir / f"{gca}.fna"), str(ref_dir / f"{gca}.fna"))
            non_ref_existing.discard(gca)
            ref_existing.add(gca)
            moved += 1
        else:
            missing_count += 1
            if len(missing_samples) < 5:
                missing_samples.append(gca)

        if idx % 10000 == 0:
            _progress("整理 ref 基因组", idx, total_ref_gcas)
    _progress("整理 ref 基因组", total_ref_gcas, total_ref_gcas)

    if missing_count:
        preview = ", ".join(missing_samples) + ("..." if missing_count > len(missing_samples) else "")
        Logger.warning(
            f"{missing_count} 个 ref 基因组在 non_ref/genomes/ 和 ref/genomes/ 中均未找到，示例: {preview}"
        )

    # ── 4. 清理 ref/genomes/ 中不再是 ref 类型的过时文件（移回 non_ref/genomes/）──
    stale = 0
    ref_files = list(ref_dir.glob("*.fna"))
    total_ref_files = len(ref_files)
    for idx, fna in enumerate(ref_files, start=1):
        m = GCA_PATTERN.search(fna.name)
        if m and m.group(1) not in ref_gcas:
            non_ref_dir.mkdir(parents=True, exist_ok=True)
            shutil.move(str(fna), str(non_ref_dir / fna.name))
            stale += 1
        _progress("清理 ref/genomes/ 过时文件", idx, total_ref_files)

    # ── 5. 合并两个 md5sums 文件，按 ref/非 ref 重新分类写入 ──────────────────
    all_md5: dict[str, str] = {}
    for md5_file in (non_ref_md5, ref_md5):
        if md5_file.exists():
            md5_count = 0
            for line in md5_file.open():
                md5_count += 1
                parts = line.strip().split()
                if len(parts) == 2:
                    all_md5[parts[0]] = parts[1]
                _tick(f"合并 {md5_file.name}", md5_count)
            Logger.info(f"[ref links] 合并 {md5_file.name} 完成 {md5_count}")

    ref_md5.parent.mkdir(parents=True, exist_ok=True)
    non_ref_md5.parent.mkdir(parents=True, exist_ok=True)
    ref_md5.write_text(
        "".join(f"{g}\t{m}\n" for g, m in sorted(all_md5.items()) if g in ref_gcas)
    )
    non_ref_md5.write_text(
        "".join(f"{g}\t{m}\n" for g, m in sorted(all_md5.items()) if g not in ref_gcas)
    )

    # ── 6. 生成 ref/non_ref metadata（流式分流，避免全量驻留内存）──────────────
    ref_meta_file = ref_metadata_path(genome_dir)
    ref_meta_file.parent.mkdir(parents=True, exist_ok=True)
    tmp_meta = ref_meta_file.with_suffix(".tmp")
    tmp_non_ref = meta_file.with_suffix(".tmp")
    split_count = 0
    with meta_file.open() as src, tmp_meta.open("w") as ref_out, tmp_non_ref.open("w") as non_ref_out:
        src_header = src.readline()
        out_header = header_line if header_line else src_header
        ref_out.write(out_header)
        non_ref_out.write(out_header)
        for line in src:
            split_count += 1
            cols = line.strip().split("\t")
            gca = cols[0] if cols else ""
            if gca and gca in ref_gcas:
                ref_out.write(line)
            else:
                non_ref_out.write(line)
            _tick("分流 ref/non_ref metadata", split_count)
    Logger.info(f"[ref links] 分流 ref/non_ref metadata 完成 {split_count}")

    tmp_meta.replace(ref_meta_file)
    tmp_non_ref.replace(meta_file)

    Logger.info(
        f"ref/genomes 已更新：移动 {moved} 个文件，清除过时 {stale} 个，"
        f"ref 基因组 {len(ref_gcas)} 个"
    )
    return moved


def update_repository(
    work_dir: Path | None,
    genome_dir: Path,
    deprecated_file: Path | None,
) -> None:
    """将处理完成的文件归档到 non_ref/genomes/ 目录，并移除已废弃基因组。

    操作步骤：
        1. 将 work_dir 中 BLAST 构建完成的文件移动到 ``genome_dir/non_ref/genomes/``
         （work_dir 为 None 时跳过此步骤，仅处理废弃逻辑）
        2. 从 non_ref/genomes/ 与 ref/genomes/ 删除废弃 GCA 的所有关联文件
        3. 从 non_ref/md5sums.txt 与 ref/md5sums.txt 删除废弃 GCA 的记录

    Args:
        work_dir:        validate_and_process_genomes 返回的工作目录（可为 None）。
        genome_dir:      最终基因组存储根目录。
        deprecated_file: 废弃 GCA 列表文件（可为 None）。
    """
    all_dest_dir = non_ref_genomes_dir(genome_dir)
    all_dest_dir.mkdir(parents=True, exist_ok=True)

    # ── 1. 按 GCA 分组，归档已完成 BLAST 构建的文件 ──────────────────────────
    if work_dir is not None:
        Logger.info("步骤 [1/3] 归档已完成 BLAST 构建的文件...")
        groups: dict[str, list[Path]] = defaultdict(list)
        for entry in os.scandir(work_dir):
            m = GCA_PATTERN.search(entry.name)
            if m:
                groups[m.group(1)].append(Path(entry.path))

        moved_count = 0
        for gca, files in groups.items():
            if not any(f.name.endswith(".blastdb_done") for f in files):
                Logger.info(f"{gca} makeblastdb 未完成，检查压缩文件完整性...")
                gz_candidates = [f for f in files if f.name.endswith(".gz")]
                if gz_candidates:
                    check_gz_integrity(gz_candidates[0])
                continue
            for f in files:
                if not f.name.endswith(".blastdb_done"):
                    dest = all_dest_dir / f.name
                    if dest.exists():
                        dest.unlink()
                    shutil.move(str(f), str(dest))
            moved_count += 1

        Logger.info(f"步骤 [1/3] 完成：已归档 {moved_count} 个新基因组到 {all_dest_dir}")
    else:
        Logger.info("步骤 [1/3] 跳过：work_dir 为空，无需归档新基因组")

    # ── 2. 将废弃 GCA 的所有关联文件移动到 deprecated/ 目录 ────────────────────
    if deprecated_file and deprecated_file.exists():
        deprecated_gcas = {ln.strip() for ln in deprecated_file.open() if ln.strip()}
        deprecated_dir = genome_dir / "deprecated"
        d_count = 0
        if deprecated_gcas:
            Logger.info(
                f"步骤 [2/3] 移动废弃 GCA 文件（共 {len(deprecated_gcas)} 个废弃 GCA）..."
            )
            deprecated_dir.mkdir(parents=True, exist_ok=True)
        else:
            Logger.info("步骤 [2/3] 跳过：废弃 GCA 列表为空")

        # 检查 non_ref/genomes/ 和 ref/genomes/ 两个目录
        for search_dir in (all_dest_dir, ref_genomes_dir(genome_dir)):
            if not search_dir.exists() or not deprecated_gcas:
                continue

            Logger.info(f" 扫描 {search_dir.name}/ 中的废弃文件（目录可能含数百万文件，请耐心等待）...")
            scanned = 0
            moved_here = 0
            _log_interval = 500_000
            for entry in os.scandir(search_dir):
                if not entry.is_file():
                    continue
                scanned += 1
                m = GCA_PATTERN.search(entry.name)
                if m and m.group(1) in deprecated_gcas:
                    shutil.move(entry.path, str(deprecated_dir / entry.name))
                    d_count += 1
                    moved_here += 1
                if scanned % _log_interval == 0:
                    Logger.info(
                        f" {search_dir.name}/ 已扫描 {scanned:,} 个文件，"
                        f"已移动 {moved_here} 个废弃文件..."
                    )
            Logger.info(
                f" {search_dir.name}/ 扫描完成：共 {scanned:,} 个文件，"
                f"移动废弃文件 {moved_here} 个"
            )

        if d_count:
            Logger.info(f"步骤 [2/3] 完成：已将 {d_count} 个废弃 GCA 文件移动到 {deprecated_dir}")
        elif deprecated_gcas:
            Logger.info("步骤 [2/3] 完成：未找到需要移动的废弃文件")

        # ── 3. 同步两个 md5sums 文件 ──────────────────────────────────────────
        Logger.info("步骤 [3/3] 清理 md5sums 文件中的废弃记录...")
        for md5sums_file in (non_ref_md5_path(genome_dir), ref_md5_path(genome_dir)):
            if not md5sums_file.exists():
                continue
            kept: list[str] = []
            removed = 0
            lines = md5sums_file.read_text().splitlines(True)
            total = len(lines)
            Logger.info(f" 处理 {md5sums_file.name}（共 {total:,} 条记录）...")
            _log_interval = max(500_000, total // 10)
            for idx, line in enumerate(lines, start=1):
                parts = line.strip().split()
                if parts and parts[0] in deprecated_gcas:
                    removed += 1
                else:
                    kept.append(line)
                if idx % _log_interval == 0:
                    Logger.info(
                        f" {md5sums_file.name} 已处理 {idx:,}/{total:,} 条，"
                        f"已删除 {removed} 条废弃记录..."
                    )
            if removed:
                md5sums_file.write_text("".join(kept))
                Logger.info(f" {md5sums_file.name} 已删除 {removed} 条废弃记录")
            else:
                Logger.info(f" {md5sums_file.name} 无废弃记录，无需修改")
        Logger.info("步骤 [3/3] 完成")
    else:
        Logger.info("步骤 [2/3] 跳过：无废弃 GCA 文件")
        Logger.info("步骤 [3/3] 跳过：无废弃 GCA 记录")


def update_metadata_table(
    clean_tsv_file: Path,
    taxonomy_dir: Path | None,
    genome_dir: Path,
    genome_type: str,
) -> None:
    """Step 6: 合并 Taxonomy 信息，生成/更新 ``non_ref/genomes_metadata.tsv``。

    Lineage 只允许来自本次下载的 taxonomy_summary.tsv，不复用旧 non_ref/genomes_metadata.tsv 中的缓存值。

    输出格式（带表头）：
        GCA  Organism  Length  TaxID  Lineage  Type

    Args:
        clean_tsv_file: parse_and_clean_metadata 的输出文件。
        taxonomy_dir:   download_taxonomy_info 返回的目录（可为 None）。
        genome_dir:     最终基因组存储根目录。
        genome_type:    'ref' 或 'all'（影响 Type 列的赋值逻辑）。
    """
    Logger.info("生成最终元数据表...")
    genome_dir.mkdir(parents=True, exist_ok=True)
    target_meta = non_ref_metadata_path(genome_dir)
    tmp         = target_meta.with_suffix(".tmp")
    target_meta.parent.mkdir(parents=True, exist_ok=True)

    # 1. 加载本次下载的 Taxonomy（TaxID → Lineage）
    tax_info: dict[str, str] = {}
    if taxonomy_dir:
        tax_summary = taxonomy_dir / "ncbi_dataset" / "data" / "taxonomy_summary.tsv"
        if tax_summary.exists():
            with tax_summary.open("r") as f:
                f.readline()   # 跳过表头
                for line in f:
                    cols = line.strip().split("\t")
                    if len(cols) > 23:
                        tid   = cols[1]
                        ranks = "|".join(cols[10:24])
                        tax_info[tid] = ranks

    # 2. 合并并写入（按 Length 降序）
    records: list[tuple[list[str], str, str]] = []
    with clean_tsv_file.open("r") as f:
        for line in f:
            cols            = line.strip().split("\t")
            taxid           = cols[3]
            refseq_category = cols[4] if len(cols) > 4 else ""
            lineage         = tax_info.get(taxid, "NA")
            records.append((cols[:4], lineage, refseq_category))

    records.sort(key=lambda x: int(x[0][2]), reverse=True)

    with tmp.open("w") as f:
        f.write("GCA\tOrganism\tLength\tTaxID\tLineage\tType\n")
        for base_cols, lineage, refseq_cat in records:
            # Type 必须由 refseq_category 决定，不能直接使用 genome_type，
            # 否则在复用全量 metadata.jsonl 且 genome_type=ref 时会把非参考条目误标为 ref。
            curr_type = "ref" if _is_refseq_reference_category(refseq_cat) else "all"
            f.write("\t".join(base_cols) + f"\t{lineage}\t{curr_type}\n")

    tmp.replace(target_meta)
    Logger.success(f"元数据更新完成: {target_meta}")
