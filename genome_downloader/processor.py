#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
基因组处理模块 — Step 3, 4 & 5。

函数列表
--------
build_blast_databases()     : 扫描两个正式目录，对缺少/不完整的 BLAST 库进行建库（主入口）
validate_blast_databases()  : 用 blastdbcmd -info 校验已建库数据库，自动修复或报告需修复基因组
validate_and_process_genomes() : 旧接口（已不在主流程中使用，保留备用）
"""
from __future__ import annotations

import hashlib
import os
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from .logger import Logger
from .utils import (
    non_ref_blastdb_dir,
    non_ref_genomes_dir,
    non_ref_md5_path,
    ref_blastdb_dir,
    ref_genomes_dir,
    ref_md5_path,
)


def _build_one(fna: Path, db_prefix: Path) -> tuple[str, bool, str]:
    """在单独线程中执行 makeblastdb。

    Returns:
        (gca_stem, success, error_message)
    """
    gca = fna.stem
    try:
        subprocess.run(
            [
                "makeblastdb",
                "-in",         str(fna),
                "-input_type", "fasta",
                "-title",      gca,
                "-dbtype",     "nucl",
                "-out",        str(db_prefix),
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            check=True,
        )
    except subprocess.CalledProcessError as e:
        return gca, False, e.stderr.decode(errors="replace").strip()
    except FileNotFoundError:
        return gca, False, "makeblastdb: 命令未找到"

    return gca, True, ""


def build_blast_databases(genome_dir: Path, threads: int = 1) -> tuple[int, int, int]:
    """为 genome_dir 下所有 .fna 文件构建 BLAST 数据库。

        扫描范围：
            - genome_dir/non_ref/genomes/*.fna → genome_dir/non_ref/blastdb/
            - genome_dir/ref/genomes/*.fna     → genome_dir/ref/blastdb/

    完整性检查：
      每个 .fna 在对应 blastdb/ 目录下存在 ``{stem}.nhr`` 时视为已完整。

    Returns:
        (total, built, skipped) —— .fna 文件总数、本次建库数、已完整跳过数
    """
    to_build: list[tuple[Path, Path]] = []  # (fna, db_prefix)
    skipped = 0

    scan_dirs = [
        (non_ref_genomes_dir(genome_dir), non_ref_blastdb_dir(genome_dir)),
        (ref_genomes_dir(genome_dir),     ref_blastdb_dir(genome_dir)),
    ]
    for scan_dir, blastdb_dir in scan_dirs:
        if not scan_dir.exists():
            continue
        for fna in sorted(scan_dir.glob("*.fna")):
            db_prefix = blastdb_dir / fna.stem          # blastdb/GCA_xxx.28
            nhr_file  = blastdb_dir / (fna.stem + ".nhr")  # blastdb/GCA_xxx.28.nhr

            if nhr_file.exists():
                skipped += 1
                continue
            to_build.append((fna, db_prefix))

    total = len(to_build) + skipped
    built_count = len(to_build)

    if not to_build:
        Logger.info(f"所有 {total} 个基因组的 BLAST 数据库均已完整，无需重建。")
        return total, 0, skipped

    Logger.info(
        f"共 {total} 个基因组：{built_count} 个需要建库，{skipped} 个已完整将跳过。"
    )

    # 确保 blastdb/ 目录存在
    for _, blastdb_dir in scan_dirs:
        blastdb_dir.mkdir(parents=True, exist_ok=True)

    failed: list[tuple[str, str]] = []
    done = 0
    # 每隔多少个完成打一条进度日志（数量越多间隔越稀疏）
    log_interval = max(1, min(50, built_count // 10 or 1))

    with ThreadPoolExecutor(max_workers=threads) as pool:
        futures = {pool.submit(_build_one, fna, db_prefix): fna.stem
                   for fna, db_prefix in to_build}
        for fut in as_completed(futures):
            gca, ok, err = fut.result()
            done += 1
            if not ok:
                # 失败时立即输出，方便排查
                Logger.warning(f"建库失败 [{done}/{built_count}]: {gca} — {err}")
                failed.append((gca, err))
            elif done % log_interval == 0 or done == built_count:
                pct = done * 100 // built_count
                Logger.info(f"建库进度: {done}/{built_count} ({pct}%)")

    succeeded = built_count - len(failed)
    Logger.info(f"建库汇总：成功 {succeeded}，失败 {len(failed)}，跳过 {skipped}。")

    return total, succeeded, skipped


# ─────────────────────────────────────────────────────────────
# 数据库校验 — Step 5
# ─────────────────────────────────────────────────────────────

def _check_db_valid(fna: Path, db_prefix: Path) -> tuple[str, bool, str]:
    """使用 blastdbcmd -info 校验数据库完整性。

    优先使用 PATH 中的 blastdbcmd，其次在 APP_DIR/bins/ 或脚本同级 bins/ 中查找。
    这解决了 web server 进程未执行 check_and_install_dependencies() 时工具不在 PATH
    中的问题（常见于从 UI 触发校验的场景）。

    Returns:
        (gca_stem, valid, error_message)
    """
    gca = fna.stem

    # 优先 PATH，其次 bins/ 目录
    blastdbcmd_bin = shutil.which("blastdbcmd")
    if not blastdbcmd_bin:
        app_dir = os.environ.get("APP_DIR", "").strip()
        bins_dir = (Path(app_dir) / "bins" if app_dir
                    else Path(__file__).parent.parent / "bins")
        candidate = bins_dir / "blastdbcmd"
        if candidate.exists():
            blastdbcmd_bin = str(candidate)

    if not blastdbcmd_bin:
        return gca, False, "blastdbcmd: 命令未找到，请确认工具已安装到 bins/ 目录"

    try:
        result = subprocess.run(
            [blastdbcmd_bin, "-db", str(db_prefix), "-info"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        if result.returncode == 0:
            return gca, True, ""
        return gca, False, result.stderr.decode(errors="replace").strip()
    except FileNotFoundError:
        return gca, False, "blastdbcmd: 命令未找到"


def _check_fna_md5(fna: Path, expected_md5: str | None) -> tuple[str, bool]:
    """计算本地 .fna 文件 MD5 并与期望值比对。

    若 expected_md5 为 None（无参考值），视为通过（无法校验）。

    Returns:
        (gca_stem, md5_ok)
    """
    gca = fna.stem
    if expected_md5 is None:
        return gca, True  # 无参考 MD5，无法校验，按通过处理
    hasher = hashlib.md5()
    try:
        with fna.open("rb") as f:
            while chunk := f.read(1 << 20):
                hasher.update(chunk)
        return gca, hasher.hexdigest() == expected_md5
    except OSError:
        return gca, False


def _load_md5_map(genome_dir: Path) -> dict[str, str]:
    """从 genome_dir 下的 *_md5sums.txt 文件加载 {GCA: md5} 映射。"""
    md5_map: dict[str, str] = {}
    for f in (non_ref_md5_path(genome_dir), ref_md5_path(genome_dir)):
        if not f.exists():
            continue
        for line in f.open():
            parts = line.strip().split()
            if len(parts) == 2:
                md5_map[parts[0]] = parts[1]
    return md5_map


def validate_blast_databases(
    genome_dir: Path,
    threads: int = 1,
) -> tuple[int, int, int, list[str]]:
    """验证 genome_dir 下所有已建库的 BLAST 数据库完整性，并自动修复可修复的库。

    流程：
    1. 扫描所有已有 .nhr 文件的基因组（即已建库）
    2. 并行使用 blastdbcmd -info 校验每个数据库完整性
    3. 对校验失败的库，并行校验对应 .fna 文件的 MD5
    4. MD5 匹配（或无参考值）→ 重建 BLAST 数据库
    5. MD5 不匹配 → 记录为需修复基因组

    Returns:
        (total, valid, rebuilt, repair_needed_gcas)
        - total: 检查的数据库总数
        - valid: 校验通过的数量
        - rebuilt: 本次成功重建的数量
        - repair_needed_gcas: MD5 不匹配、需修复基因组文件的 GCA 列表
    """
    # ── 1. 扫描所有已建库的 .fna ─────────────────────────────────────────────
    to_check: list[tuple[Path, Path]] = []  # (fna, db_prefix)
    scan_pairs = [
        (non_ref_genomes_dir(genome_dir), non_ref_blastdb_dir(genome_dir)),
        (ref_genomes_dir(genome_dir),     ref_blastdb_dir(genome_dir)),
    ]
    for scan_dir, blastdb_dir in scan_pairs:
        if not scan_dir.exists():
            continue
        for fna in sorted(scan_dir.glob("*.fna")):
            nhr_file = blastdb_dir / (fna.stem + ".nhr")
            if nhr_file.exists():
                to_check.append((fna, blastdb_dir / fna.stem))

    total = len(to_check)
    if total == 0:
        Logger.info("未找到已建库的 BLAST 数据库，跳过校验。")
        return 0, 0, 0, []

    Logger.info(f"共 {total} 个已建库数据库，开始并行校验...")

    # ── 2. 并行校验数据库 ─────────────────────────────────────────────────────
    invalid_pairs: list[tuple[Path, Path]] = []
    done = 0
    with ThreadPoolExecutor(max_workers=threads) as pool:
        futures = {
            pool.submit(_check_db_valid, fna, db_prefix): (fna, db_prefix)
            for fna, db_prefix in to_check
        }
        for fut in as_completed(futures):
            gca, ok, err = fut.result()
            done += 1
            if ok:
                Logger.info(f"[{done}/{total}] 数据库校验通过: {gca}")
            else:
                Logger.warning(f"[{done}/{total}] 数据库校验失败: {gca} — {err}")
                invalid_pairs.append(futures[fut])

    valid_count = total - len(invalid_pairs)
    if not invalid_pairs:
        Logger.info(f"所有 {total} 个数据库校验通过。")
        return total, valid_count, 0, []

    Logger.info(
        f"{len(invalid_pairs)} 个数据库存在问题，正在并行校验对应基因组文件 MD5..."
    )

    # ── 3. 加载 MD5 映射，并行校验 .fna 文件 ────────────────────────────────
    md5_map = _load_md5_map(genome_dir)
    md5_results: dict[str, bool] = {}
    with ThreadPoolExecutor(max_workers=threads) as pool:
        futures_md5 = {
            pool.submit(_check_fna_md5, fna, md5_map.get(fna.stem)): fna.stem
            for fna, _ in invalid_pairs
        }
        for fut in as_completed(futures_md5):
            gca, md5_ok = fut.result()
            md5_results[gca] = md5_ok
            if md5_ok:
                Logger.info(f"  MD5 校验通过: {gca}（将重建数据库）")
            else:
                Logger.warning(f"  MD5 校验失败: {gca}（基因组文件需要修复）")

    # ── 4. 重建 MD5 通过的数据库 ──────────────────────────────────────────────
    to_rebuild = [
        (fna, db_prefix) for fna, db_prefix in invalid_pairs
        if md5_results.get(fna.stem, True)
    ]
    repair_needed: list[str] = [
        fna.stem for fna, _ in invalid_pairs
        if not md5_results.get(fna.stem, True)
    ]

    rebuilt = 0
    if to_rebuild:
        Logger.info(f"正在重建 {len(to_rebuild)} 个数据库...")
        done_r = 0
        with ThreadPoolExecutor(max_workers=threads) as pool:
            futures_build = {
                pool.submit(_build_one, fna, db_prefix): fna.stem
                for fna, db_prefix in to_rebuild
            }
            for fut in as_completed(futures_build):
                gca, ok, err = fut.result()
                done_r += 1
                if ok:
                    Logger.info(f"[{done_r}/{len(to_rebuild)}] 重建完成: {gca}")
                    rebuilt += 1
                else:
                    Logger.warning(
                        f"[{done_r}/{len(to_rebuild)}] 重建失败: {gca} — {err}"
                    )
                    repair_needed.append(gca)

    # ── 5. 汇总 ──────────────────────────────────────────────────────────────
    Logger.info(
        f"数据库校验汇总：通过 {valid_count}，重建成功 {rebuilt}，"
        f"需修复基因组 {len(repair_needed)}。"
    )
    if repair_needed:
        Logger.warning("以下基因组文件 MD5 不匹配，请重新下载或处理：")
        for gca in repair_needed:
            Logger.warning(f"  {gca}")

    return total, valid_count, rebuilt, repair_needed


def validate_and_process_genomes(
    src_dir: Path,
    gca_list_file: Path | None,
    threads: int = 1,
) -> Path:
    """旧接口：将 src_dir/non_ref/genomes/ 中的 .fna 硬链接到临时工作目录并建库。

    .. deprecated::
        建议改用 :func:`build_blast_databases`，直接对两个最终目录原地建库。
        此函数仅残留备用。
    """
    work_dir = src_dir.parent / "blast_build"
    if work_dir.exists():
        shutil.rmtree(work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)

    Logger.info("准备处理文件 (硬链接到工作目录)...")
    count = 0
    if gca_list_file:
        with gca_list_file.open("r") as f:
            for line in f:
                gca = line.strip()
                if not gca:
                    continue
                src = src_dir / "non_ref" / "genomes" / f"{gca}.fna"
                if not src.exists():
                    Logger.warning(f"未找到已验证文件 {src}，跳过。")
                    continue
                dst = work_dir / f"{gca}.fna"
                try:
                    os.link(src, dst)
                    count += 1
                except FileExistsError:
                    count += 1
                except OSError:
                    shutil.copy2(src, dst)
                    count += 1
    Logger.info(f"链接了 {count} 个文件到工作目录。")

    build_blast_databases(work_dir.parent, threads)
    return work_dir


# ─────────────────────────────────────────────────────────────
# FAI 索引构建
# ─────────────────────────────────────────────────────────────

def _build_fai_one(fna: Path) -> tuple[str, bool, str]:
    """在单独线程中对单个 .fna 文件执行 samtools faidx。

    Returns:
        (gca_stem, success, error_message)
    """
    gca = fna.stem
    try:
        subprocess.run(
            ["samtools", "faidx", str(fna)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            check=True,
        )
        return gca, True, ""
    except subprocess.CalledProcessError as e:
        return gca, False, e.stderr.decode(errors="replace").strip()
    except FileNotFoundError:
        return gca, False, "samtools: 命令未找到，请确认 samtools 已安装"


def build_fai_indexes(genome_dir: Path, threads: int = 1) -> tuple[int, int, int]:
    """为 genome_dir 下所有 .fna 文件创建 samtools faidx 索引（.fna.fai）。

    索引文件存放在 .fna 文件的同级目录（genomes/）。
    已存在 .fai 文件的基因组将跳过。

    扫描范围：
        - genome_dir/non_ref/genomes/*.fna
        - genome_dir/ref/genomes/*.fna

    Returns:
        (total, built, skipped) —— .fna 文件总数、本次创建索引数、已有索引跳过数
    """
    to_build: list[Path] = []
    skipped = 0

    for scan_dir in (non_ref_genomes_dir(genome_dir), ref_genomes_dir(genome_dir)):
        if not scan_dir.exists():
            continue
        for fna in sorted(scan_dir.glob("*.fna")):
            fai_file = Path(str(fna) + ".fai")
            if fai_file.exists():
                skipped += 1
            else:
                to_build.append(fna)

    total = len(to_build) + skipped
    built_count = len(to_build)

    if not to_build:
        Logger.info(f"所有 {total} 个基因组的 FAI 索引均已存在，无需重建。")
        return total, 0, skipped

    Logger.info(
        f"共 {total} 个基因组：{built_count} 个需要创建 FAI 索引，{skipped} 个已存在将跳过。"
    )

    failed: list[tuple[str, str]] = []
    done = 0
    log_interval = max(1, min(50, built_count // 10 or 1))

    with ThreadPoolExecutor(max_workers=threads) as pool:
        futures = {pool.submit(_build_fai_one, fna): fna.stem for fna in to_build}
        for fut in as_completed(futures):
            gca, ok, err = fut.result()
            done += 1
            if not ok:
                Logger.warning(f"FAI 索引失败 [{done}/{built_count}]: {gca} — {err}")
                failed.append((gca, err))
            elif done % log_interval == 0 or done == built_count:
                pct = done * 100 // built_count
                Logger.info(f"FAI 索引进度: {done}/{built_count} ({pct}%)")

    succeeded = built_count - len(failed)
    Logger.info(f"FAI 索引汇总：成功 {succeeded}，失败 {len(failed)}，跳过 {skipped}。")

    return total, succeeded, skipped
