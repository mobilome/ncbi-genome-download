#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
元数据模块 — Step 1 全流程。

函数列表
--------
fetch_genome_summary()      : 调用 datasets summary 获取 JSONL 元数据
convert_jsonl_to_tsv()      : 调用 dataformat 将 JSONL 转为 TSV
parse_and_clean_metadata()  : 解析 TSV，版本去重，输出 clean TSV
check_updates_and_plan()    : 对比本地/远程，生成下载/废弃计划文件
"""
from __future__ import annotations

import re
from pathlib import Path

from .exceptions import NoUpdatesNeeded
from .logger import Logger
from .utils import (
    GCA_PATTERN,
    non_ref_genomes_dir,
    non_ref_md5_path,
    non_ref_metadata_path,
    ref_genomes_dir,
    ref_md5_path,
    run_cmd,
)


def fetch_genome_summary(
    taxon: str,
    genome_type: str,
    output_file: Path,
    overwrite: bool = False,
    api_key: str | None = None,
) -> None:
    """Step 1: 调用 ``datasets summary`` 获取基因组元数据（JSONL 格式）。

    Args:
        taxon:       分类单元，如 fungi、bacteria。
        genome_type: 'ref' 仅参考基因组；'all' 全部。
        output_file: 输出 .jsonl 文件路径。
        overwrite:   True 则强制重新下载。
        api_key:     NCBI API Key（可选）。
    """
    if output_file.exists() and not overwrite:
        Logger.info(f"元数据文件已存在，跳过下载: {output_file}")
        return

    Logger.info(f"开始获取 {taxon} 基因组元数据 ({genome_type})...")
    tmp         = output_file.with_suffix(".jsonl.tmp")
    api_key_opt = f" --api-key {api_key}" if api_key else ""
    ref_flag    = "--reference" if genome_type == "ref" else ""
    cmd = (
        f"datasets summary genome taxon {taxon} --as-json-lines "
        f"{ref_flag}{api_key_opt} > {tmp}"
    )
    Logger.shell(cmd)
    run_cmd(cmd)
    tmp.replace(output_file)


def convert_jsonl_to_tsv(jsonl_file: Path, tsv_file: Path) -> None:
    """Step 1.1: 调用 ``dataformat`` 将 JSONL 转换为 TSV。

    输出字段（按列顺序）：
        Accession, Current Accession, Paired Accession,
        Organism Name, Seq Length, TaxID, RefSeq Category
    """
    if tsv_file.exists() and jsonl_file.exists():
        if tsv_file.stat().st_mtime >= jsonl_file.stat().st_mtime:
            Logger.info(f"TSV 已是最新，跳过转换: {tsv_file}")
            return
    fields = (
        "accession,current-accession,assminfo-paired-assm-accession,"
        "organism-name,assmstats-total-sequence-len,"
        "organism-tax-id,assminfo-refseq-category"
    )
    cmd = f"cat {jsonl_file} | dataformat tsv genome --fields {fields} > {tsv_file}"
    Logger.shell(cmd)
    run_cmd(cmd)


def parse_and_clean_metadata(tsv_file: Path, clean_tsv_file: Path) -> int:
    """Step 1.2: 解析 TSV，版本去重，输出规范化的 clean TSV。

    输出格式（每行 5 列，无表头）：
        GCA  Organism  Length  TaxID  RefseqCategory

    Args:
        tsv_file:       dataformat 输出的原始 TSV。
        clean_tsv_file: 清洗后写入的目标路径。

    Returns:
        有效基因组记录数（0 表示无有效数据）。
    """
    Logger.info("解析并格式化元数据...")
    best_genomes: dict[str, dict] = {}

    with tsv_file.open("r") as fin:
        try:
            header = next(fin)   # 跳过/检查表头行
            if not header.startswith("GCA_"):
                pass             # 正常表头，已跳过
        except StopIteration:
            Logger.warning("TSV 文件为空")
            return 0

        for line in fin:
            parts = line.strip().split("\t")
            if len(parts) < 6:
                continue

            # 从前三列中取 GCA
            gca = next(
                (x for x in parts[:3] if x.upper().startswith("GCA_")), None
            )
            if not gca:
                Logger.warning(f"GCA Accession 不存在，只有 {parts[0]}")
                continue

            organism_name    = re.sub(r"\s+", "_", parts[3].strip())
            refseq_category  = parts[6].strip() if len(parts) > 6 else ""
            try:
                seq_len = int(parts[4])
                taxid   = int(parts[5])
            except ValueError:
                continue

            # 版本去重：保留版本号最大的记录
            if "." in gca:
                base_acc, ver_str = gca.rsplit(".", 1)
                try:
                    ver = int(ver_str)
                except ValueError:
                    ver = 0
            else:
                base_acc, ver = gca, 0

            record = {
                "gca": gca, "organism_name": organism_name,
                "seq_len": seq_len, "taxid": taxid,
                "refseq_category": refseq_category, "ver": ver,
            }

            if base_acc not in best_genomes or ver > best_genomes[base_acc]["ver"]:
                best_genomes[base_acc] = record

    valid_count = 0
    with clean_tsv_file.open("w") as fout:
        for base_acc in sorted(best_genomes.keys()):
            rec = best_genomes[base_acc]
            fout.write(
                f"{rec['gca']}\t{rec['organism_name']}\t"
                f"{rec['seq_len']}\t{rec['taxid']}\t{rec['refseq_category']}\n"
            )
            valid_count += 1

    Logger.success(f"清洗完成，共获取有效基因组记录: {valid_count}")
    return valid_count


def check_updates_and_plan(
    clean_tsv_file: Path,
    genome_dir: Path,
    predownload_dir: Path | None = None,
) -> tuple[Path | None, Path | None, Path | None]:
    """Step 1.3: 对比本地与远程，生成下载/废弃计划文件。

    Returns:
        (list_file, taxid_list_file, deprecated_file)
        - list_file:       待下载的 GCA 列表文件（None = 无需下载）
        - taxid_list_file: 待下载 TaxID 列表文件（None = 无需下载）
        - deprecated_file: 已废弃 GCA 列表文件（None = 无废弃）

    Raises:
        NoUpdatesNeeded: 本地已是最新且无废弃基因组，无需任何操作。
    """
    # 读取远程列表 {GCA: TaxID}
    remote_genomes: dict[str, str] = {}
    with clean_tsv_file.open("r") as f:
        for line in f:
            cols = line.strip().split("\t")
            if len(cols) >= 4:
                remote_genomes[cols[0]] = cols[3]

    # ── 读取本地已有基因组：优先从 md5sums 文件，备选扫描实际 .fna 文件 ──────
    # 因为 md5sums 文件可能在某些情况下丢失或为空，但 .fna 文件依然存在
    # 所以我们需要同时检查两个数据源，以避免误判为"本地无基因组"导致重复下载
    local_genomes: set[str] = set()
    found_any_md5 = False
    
    # Step 1: 从 md5sums 文件读取（可靠性高）
    for md5sums_file in (non_ref_md5_path(genome_dir), ref_md5_path(genome_dir)):
        if md5sums_file.exists():
            with md5sums_file.open("r") as f:
                for line in f:
                    parts = line.strip().split()   # 兼容空格和 tab 分隔
                    if parts and parts[0]:
                        local_genomes.add(parts[0])
            found_any_md5 = True
    
    # Step 2: 扫描实际的 .fna 文件（备选，处理 md5sums 丢失的情况）
    # 这确保即使 md5sums 文件损坏/丢失，程序仍能检测到已有的基因组
    fna_from_disk: set[str] = set()
    for dir_path in (non_ref_genomes_dir(genome_dir), ref_genomes_dir(genome_dir)):
        if dir_path.exists():
            for fna in dir_path.glob("*.fna"):
                m = GCA_PATTERN.search(fna.name)
                if m:
                    fna_from_disk.add(m.group(1))
    
    # 合并两个数据源，取并集（因为 md5sums 可能不完整）
    local_genomes |= fna_from_disk
    
    if local_genomes:
        Logger.info(f"本地已有基因组数量: {len(local_genomes)}")
        if fna_from_disk and not found_any_md5:
            Logger.info(f"  (从 .fna 文件扫描发现，未找到 md5sums 文件)")
        elif fna_from_disk and len(fna_from_disk) > len(local_genomes - fna_from_disk):
            Logger.info(f"  (md5sums 文件 + 磁盘扫描 合并)")
    elif not found_any_md5 and not fna_from_disk:
        Logger.warning("未找到 md5sums 文件和 .fna 文件，视为首次运行或全量更新。")

    # 预下载暂存区中的基因组视为已处理，排除出待下载列表
    if predownload_dir and predownload_dir.exists():
        predownloaded = {
            m.group(1)
            for fna in predownload_dir.glob("*.fna")
            if (m := GCA_PATTERN.search(fna.name))
        }
        if predownloaded:
            local_genomes |= predownloaded
            Logger.info(f"预下载暂存区已有基因组: {len(predownloaded)} 个")

    all_remote  = set(remote_genomes.keys())
    to_download = all_remote - local_genomes
    deprecated  = local_genomes - all_remote

    # ── 生成计划文件 ─────────────────────────────────────────────────────────
    list_file        = clean_tsv_file.parent / "download_list.txt"
    taxid_list_file  = clean_tsv_file.parent / "taxid_list.txt"
    deprecated_file  = clean_tsv_file.parent / "deprecated_list.txt"

    with list_file.open("w") as f:
        for gca in sorted(to_download):
            f.write(f"{gca}\n")

    # Taxonomy 每次都全量刷新，不做增量；只要远程返回过 TaxID，就全部重新下载。
    all_taxids: set[str] = {tid for tid in remote_genomes.values() if tid}
    with taxid_list_file.open("w") as f:
        for tid in sorted(all_taxids):
            f.write(f"{tid}\n")

    with deprecated_file.open("w") as f:
        for gca in sorted(deprecated):
            f.write(f"{gca}\n")

    if not to_download and not all_taxids:
        Logger.info("本地数据已是最新，无需下载。")
        old_list_file = clean_tsv_file.parent / "download_list.txt"
        old_taxid_file = clean_tsv_file.parent / "taxid_list.txt"
        for _f in (old_list_file, old_taxid_file):
            _f.write_text("")
        if deprecated:
            Logger.warning(f"发现 {len(deprecated)} 个已废弃/移除的基因组。")
            return None, None, deprecated_file
        raise NoUpdatesNeeded("本地数据已是最新，无废弃记录，无需任何操作。")

    if not to_download and all_taxids:
        Logger.info("本地基因组已是最新，但将全量刷新 Taxonomy。")

    Logger.info(f"计划下载基因组数: {len(to_download)}")
    Logger.info(
        f"需刷新 TaxID 数: {len(all_taxids)} (将全量重新下载 Taxonomy)"
    )
    Logger.info(f"已废弃基因组数: {len(deprecated)}")

    return list_file, taxid_list_file, deprecated_file
