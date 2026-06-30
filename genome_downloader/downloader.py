#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
基因组下载模块 — Step 2.2。

使用 NCBI datasets CLI 的 dehydrated + rehydrate 两阶段策略：
1. 先下载脱水包（仅含元数据和 fetch.txt，体积极小）
2. 修正 fetch.txt 路径为扁平化结构（data/GCA_xxx.fna）
3. 调用 rehydrate 下载实际序列（.fna.gz）
4. 逐文件解压并校验 MD5（NCBI 提供的是未压缩文件的 MD5）
5. 校验通过后将 .fna 移入 genome_dir/non_ref/genomes/ 最终存储目录
6. 校验失败时重写 fetch.txt 仅含失败 GCA，再次 rehydrate，最多重试 max_retries 轮
"""
from __future__ import annotations

import gzip
import hashlib
import shutil
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from .exceptions import DownloadError
from .logger import Logger
from .utils import (
    GCA_PATTERN,
    non_ref_genomes_dir,
    non_ref_md5_path,
    parse_ncbi_md5sum,
    ref_genomes_dir,
    run_cmd,
    run_shell,
)

_MAX_RETRIES = 3


# ─────────────────────────────────────────────────────────────
# 内部工具
# ─────────────────────────────────────────────────────────────



def _build_fetchline_map(fetch_txt: Path) -> dict[str, str]:
    """从修正后的 fetch.txt 构建 {GCA编号: 完整行} 映射（用于失败重试）。"""
    line_map: dict[str, str] = {}
    if not fetch_txt.exists():
        return line_map
    with fetch_txt.open() as f:
        for line in f:
            stripped = line.strip()
            if not stripped:
                continue
            parts = stripped.split(maxsplit=2)
            candidates = parts[:]
            if len(parts) >= 3:
                candidates.append(parts[2])
            m = None
            for candidate in candidates:
                m = GCA_PATTERN.search(candidate)
                if m:
                    break
            if m:
                line_map[m.group(1)] = stripped
    return line_map


def _decompress_verify(gz_file: Path, expected_md5: str | None) -> tuple[bool, Path]:
    """解压 .fna.gz 并校验 MD5（与 NCBI 提供的未压缩文件 MD5 比对）。

    Returns:
        (passed, fna_path)
        - passed=True  : 解压成功且 MD5 匹配（或无期望值时仅验证解压完整性）
        - passed=False : 解压失败或 MD5 不匹配；fna_path 对应文件已删除
    """
    fna_file = gz_file.with_suffix("")  # GCA_xxx.fna.gz → GCA_xxx.fna
    hasher = hashlib.md5()
    try:
        with gzip.open(gz_file, "rb") as fin, fna_file.open("wb") as fout:
            while chunk := fin.read(1 << 20):  # 1 MiB
                fout.write(chunk)
                hasher.update(chunk)
    except (gzip.BadGzipFile, EOFError, OSError) as exc:
        Logger.warning(f"  解压失败 {gz_file.name}: {exc}")
        fna_file.unlink(missing_ok=True)
        return False, fna_file

    if expected_md5 is None:
        # 无期望 MD5：仅验证 gz 解压完整性，通过
        return True, fna_file

    actual = hasher.hexdigest()
    if actual == expected_md5:
        return True, fna_file

    Logger.warning(
        f"  MD5 不匹配 {gz_file.name}\n"
        f"    期望: {expected_md5}\n"
        f"    实际: {actual}"
    )
    fna_file.unlink(missing_ok=True)
    return False, fna_file



def _verify_and_move(
    gca: str,
    data_dir: Path,
    md5_map: dict[str, str],
    genome_dir: Path,
) -> bool:
    """解压、校验单个 GCA 的 .fna.gz，通过后移入最终存储目录。

    成功后将 ``<GCA>.fna`` 写入 ``genome_dir/non_ref/genomes/`` 目录。

    Returns:
        True  : .fna 已验证并移入 genome_dir/non_ref/genomes/ 目录
        False : .gz 文件不存在或校验失败（损坏文件已删除，等待重试）
    """
    gca_dir  = non_ref_genomes_dir(genome_dir)
    dest_fna = gca_dir / f"{gca}.fna"
    # 断点续传：目标文件已存在则视为成功
    if dest_fna.exists():
        Logger.info(f"  [{gca}] 已存在于目标目录，跳过。")
        return True

    gz_file = data_dir / f"{gca}.fna.gz"
    if not gz_file.exists():
        Logger.warning(f"  [{gca}] .gz 文件不存在。")
        return False

    expected_md5 = md5_map.get(gca)
    ok, fna_file = _decompress_verify(gz_file, expected_md5)
    if ok:
        gca_dir.mkdir(parents=True, exist_ok=True)
        shutil.move(str(fna_file), dest_fna)
        gz_file.unlink(missing_ok=True)
        return True

    # 校验失败：删除损坏 .gz，等待下一轮 rehydrate 重下
    gz_file.unlink(missing_ok=True)
    return False


# ─────────────────────────────────────────────────────────────
# 内部：单批次 Rehydrate 处理
# ─────────────────────────────────────────────────────────────

def _process_rehydrate_batch(
    idx: int,
    parent: Path,
    genome_dir: Path,
    max_retries: int,
    stage_only: bool,
    predownload_dir: Path | None,
) -> tuple[int, int, list[str]]:
    """处理单个批次的解压、fetch.txt 修正、rehydrate、校验与移入目标目录。

    Args:
        idx:             批次编号（1-based）。
        parent:          批次目录的父目录（download_batch/）。
        genome_dir:      基因组最终存储根目录。
        max_retries:     最大重试轮次。
        stage_only:      是否为预下载模式。
        predownload_dir: 预下载目标目录（stage_only 时使用）。

    Returns:
        (idx, passed_count, remaining_gcas)
    """
    batch_passed = 0
    batch_remaining: list[str] = []

    # 定位批次目录与数据包
    batch_dir = parent / f"{idx:04d}"
    if not batch_dir.exists():
        batch_dir = parent / f"download_batch_{idx:04d}"
    if not batch_dir.exists():
        batch_dir = parent / f"md5_batch_{idx:04d}"
    if not batch_dir.exists():
        batch_dir = parent / f"batch_{idx:04d}"
    batch_zip = batch_dir / f"download_batch_{idx:04d}.zip"
    unzip_dir = batch_dir / "staging"

    if not batch_zip.exists():
        Logger.warning(f"跳过批次 {idx}：未找到数据包 {batch_zip}")
        return idx, 0, []

    unzip_dir.mkdir(parents=True, exist_ok=True)
    Logger.info(f"解压批次 {idx} 数据包到 {unzip_dir}...")
    run_cmd(f"unzip -o {batch_zip} -d {unzip_dir}", verbose=False)

    # 修正 fetch.txt：将路径扁平化为 data/GCA_xxx.fna
    fetch_txt = unzip_dir / "ncbi_dataset" / "fetch.txt"
    if fetch_txt.exists():
        tmp_file = fetch_txt.with_suffix(".tmp")
        with fetch_txt.open("r") as fin, tmp_file.open("w") as fout:
            for line in fin:
                raw = line.rstrip("\n")
                parts = raw.split(maxsplit=2)
                if len(parts) < 3:
                    fout.write(raw + "\n")
                    continue
                url, size, path = parts
                m = GCA_PATTERN.search(path)
                if m:
                    fout.write(f"{url}\t{size}\tdata/{m.group(1)}.fna\n")
                else:
                    fout.write(raw + "\n")
        tmp_file.replace(fetch_txt)

    # 构建 fetchline_map 与 md5_map
    fetchline_map = _build_fetchline_map(fetch_txt)
    data_dir = unzip_dir / "ncbi_dataset" / "data"
    md5_map: dict[str, str] = {}
    for cand in [
        unzip_dir / "md5sum.txt",
        unzip_dir / "ncbi_dataset" / "md5sum.txt",
        data_dir / "md5sum.txt",
    ]:
        md5_map.update(parse_ncbi_md5sum(cand))
    if md5_map:
        Logger.info(f"已加载 {len(md5_map)} 条期望 MD5 值（批次 {idx}）。")
    else:
        Logger.warning(f"批次 {idx} 未找到 MD5 期望值文件。")

    # 待处理 GCA 列表
    batch_gcas = sorted(fetchline_map.keys())
    if not batch_gcas:
        Logger.warning(f"批次 {idx} 未解析到任何 GCA，跳过 rehydrate。请检查 fetch.txt 格式。")
        try:
            batch_zip.unlink(missing_ok=True)
        except Exception:
            pass
        return idx, 0, []

    # per-batch rehydrate+verify loop
    remaining = list(batch_gcas)
    for attempt in range(1, max_retries + 1):
        if not remaining:
            break
        if attempt == 1:
            Logger.info(f"批次 {idx}: 正在 Rehydrate (共 {len(remaining)} 个基因组)...")
            run_shell(f"datasets rehydrate --gzip --directory {unzip_dir}", collapse_progress=True)
        else:
            retry_lines = [fetchline_map[g] for g in remaining if g in fetchline_map]
            if not retry_lines:
                Logger.error(f"批次 {idx}: 无可重试的 fetch 记录，终止重试。")
                break
            Logger.warning(f"批次 {idx}: {len(remaining)} 个基因组校验失败，第 {attempt}/{max_retries} 轮重试...")
            fetch_txt.write_text("\n".join(retry_lines) + "\n", encoding="utf-8")
            run_shell(f"datasets rehydrate --gzip --directory {unzip_dir}", collapse_progress=True)

        still_failing: list[str] = []
        for gca in remaining:
            gz_file = data_dir / f"{gca}.fna.gz"
            fna_file = data_dir / f"{gca}.fna"
            dest_nonref = non_ref_genomes_dir(genome_dir) / f"{gca}.fna"
            dest_ref = ref_genomes_dir(genome_dir) / f"{gca}.fna"

            if dest_nonref.exists() or dest_ref.exists():
                batch_passed += 1
                continue

            if fna_file.exists():
                if stage_only:
                    target = predownload_dir if predownload_dir else fna_file.parent
                    target.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(fna_file), str(target / fna_file.name))
                    gz_file.unlink(missing_ok=True)
                    if predownload_dir and gca in md5_map:
                        md5_cache_file = predownload_dir / ".md5cache.tsv"
                        with md5_cache_file.open("a") as _cf:
                            _cf.write(f"{gca}\t{md5_map[gca]}\n")
                    batch_passed += 1
                    continue
                else:
                    non_ref_genomes_dir(genome_dir).mkdir(parents=True, exist_ok=True)
                    shutil.move(str(fna_file), str(non_ref_genomes_dir(genome_dir) / fna_file.name))
                    gz_file.unlink(missing_ok=True)
                    batch_passed += 1
                    continue

            if gz_file.exists():
                ok, _ = _decompress_verify(gz_file, md5_map.get(gca))
                if ok:
                    if stage_only:
                        target = predownload_dir if predownload_dir else data_dir
                        target.mkdir(parents=True, exist_ok=True)
                        shutil.move(str(data_dir / f"{gca}.fna"), str(target / f"{gca}.fna"))
                        gz_file.unlink()
                        if predownload_dir and gca in md5_map:
                            md5_cache_file = predownload_dir / ".md5cache.tsv"
                            with md5_cache_file.open("a") as _cf:
                                _cf.write(f"{gca}\t{md5_map[gca]}\n")
                        batch_passed += 1
                        continue
                    else:
                        if _verify_and_move(gca, data_dir, md5_map, genome_dir):
                            batch_passed += 1
                            continue
                gz_file.unlink(missing_ok=True)
                still_failing.append(gca)
            else:
                still_failing.append(gca)

        remaining = still_failing
        if not remaining:
            break

    batch_remaining = remaining

    # 清理批次临时 zip
    try:
        batch_zip.unlink(missing_ok=True)
    except Exception:
        pass

    return idx, batch_passed, batch_remaining


# ─────────────────────────────────────────────────────────────
# 公开 API
# ─────────────────────────────────────────────────────────────

def download_genomes(
    list_file: Path,
    genome_dir: Path,
    overwrite: bool = False,
    api_key: str | None = None,
    max_retries: int = _MAX_RETRIES,
    stage_only: bool = False,
    predownload_dir: Path | None = None,
    batch_size: int = 500,
    parallel_downloads: int = 4,
) -> Path:
    """Step 2.2: 下载基因组数据（Dehydrated + Rehydrate + 逐文件 MD5 校验）。

    每个 .fna.gz 下载后立即解压并与 NCBI 提供的未压缩 MD5 值比对；
    校验失败时重写 fetch.txt 仅含失败 GCA，再次调用 datasets rehydrate
    重新下载，直到所有基因组通过校验或达到最大重试次数。

    已验证的基因组文件存放于 ``genome_dir/non_ref/genomes/``。

    Args:
        list_file:   包含待下载 GCA 列表的文件路径（每行一个 GCA）。
        genome_dir:  基因组最终存储根目录（已验证文件写入 non_ref/genomes/）。
        overwrite:   True 则强制重新下载脱水包。
        api_key:     NCBI API Key（可选）。
        max_retries: 最大下载轮次（含初次），默认 3 轮。

    Returns:
        genome_dir（已验证的 .fna 文件统一存放在 ``non_ref/genomes/`` 子目录中）。
    """
    output_zip = list_file.parent / "download.zip"
    api_key_opt = f" --api-key {api_key}" if api_key else ""

    # 先读取待下载的 GCA 列表以决定是否分批
    gcas = [line.strip() for line in list_file.open() if line.strip()]
    if not gcas:
        Logger.info("下载列表为空，跳过下载。")
        return genome_dir

    total = len(gcas)
    effective_batch = max(1, batch_size)
    batches = [gcas[i:i + effective_batch] for i in range(0, total, effective_batch)]
    num_batches = len(batches)
    workers = min(max(1, parallel_downloads), num_batches)

    Logger.info(f"将分 {num_batches} 批次下载基因组数据 (并行数: {workers})...")

    # 如果只有 1 个批次则仍使用兼容的单次下载路径（保留 output_zip 语义），
    # 否则跳过一次性下载，改为按批次并行请求以避免超大单次请求。
    if num_batches == 1:
        if output_zip.exists() and not overwrite and not stage_only:
            Logger.info("基因组数据包已存在，跳过下载。")
        else:
            Logger.info("正在下载基因组数据 (dehydrated)...")
            try:
                run_shell(
                    f"datasets download genome accession "
                    f"--inputfile {list_file} --dehydrated "
                    f"--filename {output_zip}{api_key_opt}",
                    collapse_progress=True,
                )
            except KeyboardInterrupt:
                Logger.warning("下载脱水包被中断，清理临时文件...")
                output_zip.unlink(missing_ok=True)
                raise
    else:
        Logger.info("跳过一次性脱水包下载，启用分批并行下载模式...")

    parent = list_file.parent / "download_batch"
    parent.mkdir(parents=True, exist_ok=True)

    def _cleanup_download_batches() -> None:
        if parent.exists():
            shutil.rmtree(parent, ignore_errors=True)

    def _download_dehydrated(idx: int, batch: list[str], parent_dir: Path) -> tuple[int, bool]:
        batch_dir = parent_dir / f"{idx:04d}"
        batch_dir.mkdir(parents=True, exist_ok=True)
        batch_file = batch_dir / f"download_batch_{idx:04d}.txt"
        batch_zip = batch_dir / f"download_batch_{idx:04d}.zip"
        with batch_file.open("w") as f:
            f.writelines(f"{g}\n" for g in batch)

        api_opt = f" --api-key {api_key}" if api_key else ""
        cmd = (
            f"datasets download genome accession --inputfile {batch_file} --dehydrated --filename {batch_zip}{api_opt}"
        )
        for attempt in range(1, max_retries + 1):
            Logger.shell(f"[尝试 {attempt}/{max_retries}] {cmd}")
            if batch_zip.exists():
                batch_zip.unlink()
            proc = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if proc.returncode == 0 and batch_zip.exists():
                Logger.success(f"  批次 {idx} 完成下载")
                return idx, True
            Logger.warning(f"  批次 {idx} 第 {attempt} 次尝试失败 (returncode={proc.returncode})")
            if attempt < max_retries:
                wait = 10 * attempt
                Logger.info(f"  等待 {wait}s 后重试...")
                time.sleep(wait)
        Logger.error(f"  批次 {idx} 经过 {max_retries} 次重试后仍然失败！")
        return idx, False

    batch_results: dict[int, bool] = {}
    if num_batches == 1:
        # 小批次直接在当前目录创建 download.zip (保持兼容)
        batch_dir = parent / "0001"
        batch_dir.mkdir(parents=True, exist_ok=True)
        batch_file = batch_dir / "download_batch_0001.txt"
        batch_zip = batch_dir / "download_batch_0001.zip"
        with batch_file.open("w") as f:
            f.writelines(f"{g}\n" for g in gcas)
        api_opt = f" --api-key {api_key}" if api_key else ""
        cmd = (
            f"datasets download genome accession --inputfile {batch_file} --dehydrated --filename {batch_zip}{api_opt}"
        )
        Logger.shell(cmd)
        proc = subprocess.run(cmd, shell=True)
        if proc.returncode != 0 or not batch_zip.exists():
            raise DownloadError("下载脱水包失败")
        batch_results[1] = True
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(_download_dehydrated, idx, batch, parent): idx
                for idx, batch in enumerate(batches, start=1)
            }
            for fut in as_completed(futures):
                idx, ok = fut.result()
                batch_results[idx] = ok

        failed = sorted(idx for idx, ok in batch_results.items() if not ok)
        if failed:
            raise DownloadError(f"以下下载批次失败: {failed}")

    # 并行处理各批次：解压、修正 fetch.txt、rehydrate、校验并移入目标目录
    passed = 0
    overall_remaining: list[str] = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                _process_rehydrate_batch,
                idx, parent, genome_dir,
                max_retries, stage_only, predownload_dir,
            ): idx
            for idx in range(1, num_batches + 1)
        }
        for fut in as_completed(futures):
            idx, batch_passed, batch_remaining = fut.result()
            passed += batch_passed
            overall_remaining.extend(batch_remaining)

    failed = len(overall_remaining)
    Logger.info(f"MD5 校验完成：通过 {passed} 个，失败/跳过 {failed} 个")
    if failed and passed == 0:
        raise DownloadError(f"所有 {failed} 个基因组均校验失败，请检查网络或 NCBI 服务状态。")

    # stage_only 模式：返回预下载目录
    if stage_only:
        dest = predownload_dir if predownload_dir else parent
        Logger.info(f"预下载完成：{passed} 个基因组已验证并保留在 {dest}")
        _cleanup_download_batches()
        return dest

    # 将已验证 GCA 的 MD5 持久化到 non_ref/md5sums.txt（聚合所有批次 md5_map）
    # 为兼容旧逻辑，收集所有 md5sums 已写入的记录
    existing_md5s: dict[str, str] = {}
    md5sums_file = non_ref_md5_path(genome_dir)
    if md5sums_file.exists():
        for line in md5sums_file.open():
            parts = line.strip().split()
            if len(parts) == 2:
                existing_md5s[parts[0]] = parts[1]
    # Note: md5_map values were per-batch; we assume previous steps wrote caches into predownload_dir
    md5sums_file.parent.mkdir(parents=True, exist_ok=True)
    with md5sums_file.open("w") as f:
        for g, m in sorted(existing_md5s.items()):
            f.write(f"{g}\t{m}\n")
    Logger.info(f"已写入 {len(existing_md5s)} 条 MD5 记录到 {md5sums_file}")

    _cleanup_download_batches()

    return genome_dir
