#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
'''
@File    :   mobilome_ncbi_genome_update.py
@Time    :   2026/01/14 19:55:52
@Author  :   Naisu Yang 
@Version :   1.0
@Contact :   3298990@qq.com
'''

import os
import re
import sys
import gzip
import shutil
import datetime
import argparse
import subprocess
import json
import time
from pathlib import Path
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

# 全局正则，用于匹配 GCA 编号
GCA_PATTERN = re.compile(r"(GCA_\d+\.\d+)")

class Logger:
    COLORS = {
        "INFO": "\033[97m",       # White
        "SUCCESS": "\033[92m",    # Green
        "WARNING": "\033[93m",    # Yellow
        "ERROR": "\033[91m",      # Red
        "RUN": "\033[96m",        # Cyan
        "SHELL": "\033[94m",      # Blue
        "STEP": "\033[95m",       # Magenta
    }
    RESET = "\033[0m"

    @classmethod
    def log(cls, level, msg):
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        color = cls.COLORS.get(level, "")
        print(f"{color}[{now}] [{level}] {msg}{cls.RESET}")

    @classmethod
    def info(cls, msg):
        cls.log("INFO", msg)

    @classmethod
    def success(cls, msg):
        cls.log("SUCCESS", msg)

    @classmethod
    def warning(cls, msg):
        cls.log("WARNING", msg)

    @classmethod
    def error(cls, msg):
        cls.log("ERROR", msg)

    @classmethod
    def run(cls, msg):
        cls.log("RUN", msg)

    @classmethod
    def shell(cls, msg):
        cls.log("SHELL", msg)

    @classmethod
    def step(cls, title, level="STEP", width: int = 70):
        print(f"\n{cls.COLORS.get(level, '')}{'=' * width}{cls.RESET}")
        header = f" {title} "
        pad_len = (width - len(header)) // 2
        sep = "─" * max(0, pad_len)
        print(f"{cls.COLORS.get(level, '')}{sep}{header}{sep}{cls.RESET}\n")

def run_cmd(cmd, verbose=True, shell=True):
    """
    执行 Shell 命令并返回输出。如果失败则退出程序。
    """
    try:
        result = subprocess.run(
            cmd,
            shell=shell,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        if verbose and result.stdout:
            print(result.stdout.strip())
        return result.stdout.strip()

    except subprocess.CalledProcessError as e:
        Logger.error("Command failed!")
        Logger.error(f"Command: {cmd}")
        if e.stdout:
            print(f"\n[STDOUT]\n{e.stdout.strip()}")
        if e.stderr:
            print(f"\n[STDERR]\n{e.stderr.strip()}")
        sys.exit(e.returncode)

def run_shell(cmd):
    """
    实时执行 Shell 命令（输出直接打印到终端）。
    """
    process = subprocess.Popen(
        cmd,
        shell=True,
        stdout=sys.stdout,
        stderr=sys.stderr
    )
    process.wait()

def fetch_genome_summary(taxon, genome_type, output_file, overwrite=False, api_key=None):

    """Step 1: 获取基因组元数据 (datasets summary)"""
    tmp = output_file.with_suffix(".jsonl.tmp")
    if output_file.exists() and not overwrite:
        Logger.info(f"元数据文件已存在，跳过下载: {output_file}")
        return

    Logger.info(f"开始获取 {taxon} 基因组元数据 ({genome_type})...")
    api_key_opt = f" --api-key {api_key}" if api_key else ""
    # 使用 --as-json-lines 格式
    cmd = (
        f"datasets summary genome taxon {taxon} --as-json-lines "
        f"{'--reference' if genome_type == 'ref' else ''}{api_key_opt} > {tmp}"
    )
    Logger.shell(cmd)
    run_cmd(cmd)
    tmp.replace(output_file)

def convert_jsonl_to_tsv(jsonl_file, tsv_file):
    """Step 1.1: 将 JSONL 转换为 TSV (dataformat)"""
    # 字段: Accession, Current Accession, Paired Accession, Organism Name, Seq Length, TaxID
    fields = "accession,current-accession,assminfo-paired-assm-accession,organism-name,assmstats-total-sequence-len,organism-tax-id"
    cmd = f"cat {jsonl_file} | dataformat tsv genome --fields {fields} > {tsv_file}"
    Logger.shell(cmd)
    run_cmd(cmd)

def parse_and_clean_metadata(tsv_file, clean_tsv_file):
    """Step 1.2: 解析并清洗元数据"""
    Logger.info("解析并格式化元数据...")
    valid_count = 0

    best_genomes = {}
    
    with tsv_file.open("r") as fin:
        # dataformat 输出包含表头，需要跳过
        try:
            line = next(fin)
            if not line.startswith("GCA_"):
                 pass 
        except StopIteration:
            Logger.warning("TSV 文件为空")
            return 0

        for line in fin:
            parts = line.strip().split("\t")
            if len(parts) < 6: 
                continue

            # 提取 GCA
            gca = next((x for x in parts[:3] if x.upper().startswith("GCA_")), None)
            if not gca:
                Logger.warning(f"GCA Accession 不存在，只有{parts[0]}")
                continue
            
            # 提取字段
            organism_name = re.sub(r"\s+", "_", parts[3].strip())# 替换空格
            try:
                seq_len = int(parts[4])
                taxid = int(parts[5])
            except ValueError:
                continue

            # 版本去重逻辑
            if "." in gca:
                base_acc, ver_str = gca.rsplit(".", 1)
                try:
                    ver = int(ver_str)
                except ValueError:
                    ver = 0
            else:
                base_acc = gca
                ver = 0
            
            current_record = {
                "gca": gca,
                "organism_name": organism_name,
                "seq_len": seq_len,
                "taxid": taxid,
                "ver": ver
            }

            if base_acc not in best_genomes:
                best_genomes[base_acc] = current_record
            else:
                if ver > best_genomes[base_acc]["ver"]:
                     best_genomes[base_acc] = current_record

    with clean_tsv_file.open("w") as fout:
        for base_acc in sorted(best_genomes.keys()):
            rec = best_genomes[base_acc]
            # 输出格式: GCA, Name, Length, TaxID
            fout.write(f"{rec['gca']}\t{rec['organism_name']}\t{rec['seq_len']}\t{rec['taxid']}\n")
            valid_count += 1
            
    Logger.success(f"清洗完成，共获取有效基因组记录: {valid_count}")
    return valid_count

def check_updates_and_plan(clean_tsv_file, genome_dir):


    """Step 1.3: 比较本地与远程，生成更新计划"""
    
    # 读取远程列表 {GCA: TaxID} (或其它信息)
    remote_genomes = {}
    with clean_tsv_file.open("r") as f:
        for line in f:
            cols = line.strip().split("\t")
            if len(cols) >= 4:
                remote_genomes[cols[0]] = cols[3]

    # 读取本地列表 (通过索引文件)
    local_genomes = set()
    index_file = genome_dir / "gca_file_index.tsv"
    if index_file.exists():
        with index_file.open("r") as f:
            for line in f:
                parts = line.strip().split("\t")
                if parts:
                    local_genomes.add(parts[0])
        Logger.info(f"本地已有基因组数量: {len(local_genomes)}")
    else:
        Logger.warning("未找到本地索引文件，视为首次运行或全量更新。")

    # 计算差异
    all_remote = set(remote_genomes.keys())
    to_download = all_remote - local_genomes
    deprecated = local_genomes - all_remote

    if not to_download:
        Logger.info("本地数据已是最新，无需下载。")
        if deprecated:
            Logger.warning(f"发现 {len(deprecated)} 个已废弃/移除的基因组。")
            # 写入废弃列表
            deprecated_file = clean_tsv_file.with_suffix(".deprecated")
            with deprecated_file.open("w") as f:
                for gca in sorted(deprecated):
                    f.write(f"{gca}\n")
            return None, None, deprecated_file
        sys.exit(0)

    # 生成计划文件
    list_file = clean_tsv_file.with_suffix(".genome_to_download")
    taxid_list_file = clean_tsv_file.with_suffix(".taxon_to_download")
    deprecated_file = clean_tsv_file.with_suffix(".deprecated")

    # 1. 下载列表 (GCA)
    with list_file.open("w") as f:
        for gca in sorted(to_download):
            f.write(f"{gca}\n")
    
    # 2. TaxID 列表 (唯一)
    unique_taxids = sorted({taxid for taxid in remote_genomes.values() if taxid})
    with taxid_list_file.open("w") as f:
        for tid in unique_taxids:
            f.write(f"{tid}\n")

    # 3. 废弃列表
    with deprecated_file.open("w") as f:
        for gca in sorted(deprecated):
            f.write(f"{gca}\n")

    Logger.info(f"计划下载基因组数: {len(to_download)}")
    Logger.info(f"涉及 TaxID 数: {len(unique_taxids)}")
    Logger.info(f"已废弃基因组数: {len(deprecated)}")
    
    return list_file, taxid_list_file, deprecated_file

def _download_one_taxonomy_batch(idx, total_batches, batch, parent_dir, overwrite, api_key, max_retries=3):
    """下载单个 Taxonomy 批次（供线程池调用）

    失败时自动重试，最多 max_retries 次。

    Returns:
        (idx, batch_summary_path or None)
    """
    batch_file = parent_dir / f"taxid_batch_{idx}.txt"
    batch_zip  = parent_dir / f"taxid_batch_{idx}.zip"
    batch_dir  = parent_dir / f"taxid_batch_{idx}_report"

    # 写入批次文件
    with batch_file.open("w") as f:
        for tid in batch:
            f.write(f"{tid}\n")

    Logger.info(f"[批次 {idx}/{total_batches}] 包含 {len(batch)} 个 TaxID")

    # 下载（含重试）
    if batch_zip.exists() and not overwrite:
        Logger.info(f"  批次 {idx} 数据包已存在，跳过下载。")
    else:
        api_key_opt = f" --api-key {api_key}" if api_key else ""
        cmd = f"datasets download taxonomy taxon --inputfile {batch_file} --filename {batch_zip}{api_key_opt}"

        for attempt in range(1, max_retries + 1):
            Logger.shell(f"[尝试 {attempt}/{max_retries}] {cmd}")
            # 删除可能残留的不完整 zip
            if batch_zip.exists():
                batch_zip.unlink()
            proc = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if proc.returncode == 0 and batch_zip.exists():
                break
            Logger.warning(f"  批次 {idx} 第 {attempt} 次尝试失败 (returncode={proc.returncode})")
            if attempt < max_retries:
                wait = 10 * attempt  # 递增等待: 10s, 20s, 30s
                Logger.info(f"  等待 {wait}s 后重试...")
                time.sleep(wait)

    if not batch_zip.exists():
        Logger.error(f"  批次 {idx} 经过 {max_retries} 次重试后仍然失败！")
        return (idx, None)

    # 解压
    batch_dir.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        f"unzip -o {batch_zip} -d {batch_dir}",
        shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )

    batch_summary = batch_dir / "ncbi_dataset" / "data" / "taxonomy_summary.tsv"
    if batch_summary.exists():
        Logger.success(f"  批次 {idx} 完成。")
        return (idx, batch_summary)
    else:
        Logger.error(f"  批次 {idx} 解压后未找到 taxonomy_summary.tsv！")
        return (idx, None)


def download_taxonomy_info(taxid_list_file, overwrite=False, batch_size=500, api_key=None, parallel_downloads=4):
    """Step 2.1: 下载 Taxonomy 数据（自动分批并行下载后合并）

    当 taxid 数量超过 batch_size 时，会拆分为多个子文件，
    最多 parallel_downloads 个批次同时下载，
    最后将各批次的 taxonomy_summary.tsv 合并到统一目录。
    """
    if not taxid_list_file or not taxid_list_file.exists():
        return None

    # 读取所有 TaxID
    with taxid_list_file.open("r") as f:
        all_taxids = [line.strip() for line in f if line.strip()]

    if not all_taxids:
        Logger.warning("TaxID 列表为空，跳过 Taxonomy 下载。")
        return None

    total = len(all_taxids)
    Logger.info(f"共有 {total} 个 TaxID 需要下载 Taxonomy 信息 (batch_size={batch_size})")

    # 最终合并目录
    report_dir = taxid_list_file.parent / "taxonomy_report"
    report_dir.mkdir(parents=True, exist_ok=True)
    merged_data_dir = report_dir / "ncbi_dataset" / "data"
    merged_data_dir.mkdir(parents=True, exist_ok=True)
    merged_summary = merged_data_dir / "taxonomy_summary.tsv"

    # 如果已合并且不强制覆盖，直接返回
    if merged_summary.exists() and not overwrite:
        Logger.info("Taxonomy 合并文件已存在，跳过下载。")
        return report_dir

    # 分批
    batches = [all_taxids[i:i + batch_size] for i in range(0, total, batch_size)]
    num_batches = len(batches)
    workers = min(parallel_downloads, num_batches)
    Logger.info(f"将分 {num_batches} 批次下载 Taxonomy 数据 (并行数: {workers})...")

    parent_dir = taxid_list_file.parent

    # 并行下载
    batch_results = {}  # idx -> summary_path or None
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                _download_one_taxonomy_batch,
                idx, num_batches, batch, parent_dir, overwrite, api_key
            ): idx
            for idx, batch in enumerate(batches, start=1)
        }
        for future in as_completed(futures):
            idx, summary_path = future.result()
            batch_results[idx] = summary_path

    # 检查是否有批次失败
    failed_batches = [idx for idx, path in batch_results.items() if path is None]
    if failed_batches:
        failed_batches.sort()
        Logger.error(f"以下批次下载失败: {failed_batches}")
        Logger.error("Taxonomy 数据不完整，无法继续，程序终止。")
        sys.exit(1)

    # 按批次序号顺序合并，保证结果一致性
    header_line = None
    all_data_lines = []
    for idx in sorted(batch_results.keys()):
        summary_path = batch_results[idx]
        if summary_path and summary_path.exists():
            with summary_path.open("r") as f:
                first_line = f.readline()
                if header_line is None:
                    header_line = first_line
                for line in f:
                    if line.strip():
                        all_data_lines.append(line)

    # 合并写入
    if header_line:
        with merged_summary.open("w") as f:
            f.write(header_line)
            for line in all_data_lines:
                f.write(line)
        Logger.success(f"Taxonomy 合并完成: {merged_summary} ({len(all_data_lines)} 条记录)")
    else:
        Logger.warning("所有批次均未产生有效的 taxonomy_summary.tsv")

    # 清理批次临时文件
    for idx in range(1, num_batches + 1):
        for suffix in [f"taxid_batch_{idx}.txt", f"taxid_batch_{idx}.zip"]:
            tmp = parent_dir / suffix
            if tmp.exists():
                tmp.unlink()
        tmp_dir = parent_dir / f"taxid_batch_{idx}_report"
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir)

    return report_dir

def download_genomes(list_file, overwrite=False, api_key=None):
    """Step 2.2: 下载基因组数据 (Dehydrated + Rehydrate)"""
    output_zip = list_file.parent / (list_file.name + ".zip")
    api_key_opt = f" --api-key {api_key}" if api_key else ""

    if output_zip.exists() and not overwrite:
         Logger.info("基因组数据包已存在，跳过下载。")
    else:
        Logger.info("正在下载基因组数据 (dehydrated)...")
        # 直接使用 inputfile 批量下载
        cmd = f"datasets download genome accession --inputfile {list_file} --dehydrated --filename {output_zip}{api_key_opt}"
        run_shell(cmd)

    # 解压
    unzip_dir = output_zip.parent / "genome_download_tmp"
    unzip_dir.mkdir(parents=True, exist_ok=True)

    Logger.info(f"解压数据包到 {unzip_dir}...")
    run_cmd(f"unzip -o {output_zip} -d {unzip_dir}", verbose=False)
    print(f"unzip -o {output_zip} -d {unzip_dir}")

    # 修正 fetch.txt 路径 (扁平化目录结构)
    # 默认结构: data/GCA_xxx/GCA_xxx_genomic.fna
    # 目标结构: data/GCA_xxx.fna (为了方便后续处理)
    fetch_txt = unzip_dir / "ncbi_dataset" / "fetch.txt"
    tmp_file = fetch_txt.with_suffix(".tmp")
    if fetch_txt.exists():
        with fetch_txt.open("r") as fin, tmp_file.open("w") as fout:
            for line in fin:
                line = line.rstrip("\n")
                # 最多切3列，防止路径中有空格
                parts = line.split(maxsplit=2)
                if len(parts) < 3:
                    fout.write(line + "\n")
                    continue
                
                url, size, path = parts
                # path 类似: data/GCA_000001405.28/GCA_000001405.28_GRCh38.p14_genomic.fna
                # 我们希望它 hydrate 到 data/GCA_000001405.28.fna
                m = GCA_PATTERN.search(path)
                if m:
                    gca_name = m.group(1)
                    new_path = f"data/{gca_name}.fna"
                    fout.write(f"{url}\t{size}\t{new_path}\n")
                else:
                    # fallback 保留原行
                    fout.write(line + "\n")
        # 原子替换
    tmp_file.replace(fetch_txt)
    
    Logger.info("正在 Rehydrate (下载实际序列)...")
    run_shell(f"datasets rehydrate --gzip --directory {unzip_dir}{api_key_opt}")

    # 返回包含实际 .fna.gz 的目录
    return unzip_dir / "ncbi_dataset" / "data"

def validate_and_process_genomes(src_dir, gca_list_file, threads=1):
    """Step 3 & 4: 校验、Move、Makeblastdb"""
    
    # 建立一个工作目录处理新文件
    work_dir = src_dir.parent.parent / "processing" # tmp/taxon/processing
    if work_dir.exists():
        shutil.rmtree(work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)

    Logger.info("准备处理文件 (硬链接到工作目录)...")
    
    # 只需要处理本次下载列表中的 GCA
    with gca_list_file.open("r") as f:
        count = 0
        for gca in f:
            gz_file_name = gca.strip() + ".fna.gz"
            src = src_dir / gz_file_name
            dst = work_dir / gz_file_name
            try:
                os.link(src, dst)
                count += 1
            except FileExistsError:
                pass
    
    Logger.info(f"链接了 {count} 个文件到工作目录。")

    # 并行解压
    Logger.info("并行解压...")
    cmd = f"find {work_dir} -name '*.gz' | parallel -j {threads} --bar gunzip -f {{}}"
    run_shell(cmd)

    #校验md5
    local_md5sum = work_dir.parent / "checked.md5sum.txt"
    if local_md5sum.exists():
        local_md5sum.unlink()
    cmd = (
            f"find {work_dir} -maxdepth 1 -type f -name '*.fna' | "
            f"parallel -j {threads} --bar "
            f"md5sum {{}} >> {local_md5sum}"
        )
    Logger.shell(cmd)
    run_shell(cmd)

    remote_md5sum = src_dir.parent.parent / "md5sum.txt"
    with local_md5sum.open() as f1, remote_md5sum.open() as f2:
        local_md5sum_map = {line.split()[0]: line.split()[1] for line in f1}
        remote_md5sum_set = {line.split()[0] for line in f2}

    md5fail = local_md5sum_map.keys() - remote_md5sum_set

    #删除文件
    if md5fail:
        Logger.warning(f"删除 {len(md5fail)} 个校验失败的基因组文件")
        for md5 in md5fail:
            file_path = Path(local_md5sum_map[md5])
            if file_path.exists():
                file_path.unlink()

    Logger.info("生成 BLAST 数据库...")
    
    blast_cmds = []
    for fna in work_dir.glob("*.fna"):
        db_out = fna.with_suffix("") # remove .fna, blastdb prefix
        title = fna.stem
        done_file = fna.with_suffix(".blastdb_done")
        if done_file.exists():
            continue
        # cmd: makeblastdb ... && samtools faidx ... && touch done
        # 这一步非常耗时，生成脚本并行执行
        cmd_str = (
            f"makeblastdb -in {fna} -input_type fasta -title {title} -dbtype nucl -out {db_out} > /dev/null 2>&1 && "
            f"samtools faidx {fna} && "
            f"touch {done_file}"
        )
        blast_cmds.append(cmd_str)

    cmd_file = work_dir.parent / "run_makeblastdb.sh"
    with cmd_file.open("w") as f:
        f.write("\n".join(blast_cmds))
    
    run_shell(f"cat {cmd_file} | parallel -j {threads} --bar")
    
    return work_dir

def rebuild_index(genome_dir):
    """重建本地 GCA 索引文件"""
    gca_dir = genome_dir / "GCA"
    index_file = genome_dir / "gca_file_index.tsv"
    tmp = index_file.with_suffix(".tmp")
    records = []
    if gca_dir.exists():
        for entry in os.scandir(gca_dir):
            m = GCA_PATTERN.search(entry.name)
            if m:
                # GCA \t filename
                records.append(f"{m.group(1)}\t{entry.name}\n")
    
    with tmp.open("w") as f:
        f.writelines(sorted(records))
    tmp.replace(index_file)
    
    Logger.success(f"索引已更新: {index_file} ({len(records)} 条记录)")

def update_repository(work_dir, genome_dir, deprecated_file):
    """Step 5: 将处理好的文件归档到最终目录"""
    
    final_gca_dir = genome_dir / "GCA"
    final_gca_dir.mkdir(parents=True, exist_ok=True)
    deprecated_dir = genome_dir / "deprecated"
    deprecated_dir.mkdir(parents=True, exist_ok=True)

    groups = defaultdict(list)
    # 1. 移动新文件
    moved_count = 0
    # 扫描 work_dir
    for entry in os.scandir(work_dir):
        m = GCA_PATTERN.search(entry.name)
        if m:
            gca = m.group(1)
            groups[gca].append(Path(entry))
    for gca in groups:
        if not any(f.name.endswith(".blastdb_done") for f in groups[gca]):
            Logger.info(f"{gca} makeblastdb 未完成,检查压缩文件是否完整中...")
            gz_path = groups[gca][0].with_suffix(".gz")
            check_gz_integrity(gz_path, chunk_size=1024 * 1024)
            continue

        for entry in groups[gca]:
            if not entry.name.endswith(".blastdb_done"):
                dst = final_gca_dir / entry.name
                shutil.move(str(entry), str(dst))
        moved_count += 1
    Logger.info(f"已归档 {moved_count} 个新基因组到 {final_gca_dir}")

    # 2. 处理废弃文件
    if deprecated_file and deprecated_file.exists():
        deprecated_gcas = set(line.strip() for line in deprecated_file.open())
        d_count = 0
        for entry in os.scandir(final_gca_dir):
            m = GCA_PATTERN.search(entry.name)
            if m and m.group(1) in deprecated_gcas:
                shutil.move(entry.path, deprecated_dir / entry.name)
                d_count += 1
        Logger.info(f"已移除 {d_count} 个废弃文件到 {deprecated_dir}")

    # 3. 重建索引
    rebuild_index(genome_dir)

def update_metadata_table(clean_tsv_file, taxonomy_dir, genome_dir, genome_type):


    """Step 6: 合并 Taxonomy 信息生成最终 Metadata"""
    Logger.info("生成最终元数据表...")
    
    target_meta = genome_dir / "genome_metadata.tsv"
    tmp = target_meta.with_suffix(".tmp")

    # 读取旧元数据以保留 Type=ref
    existing_ref_gcas = set()
    if target_meta.exists():
        with target_meta.open("r") as f:
            header = f.readline().strip().split("\t")
            if "Type" in header:
                type_idx = header.index("Type")
                gca_idx = 0  # Assuming GCA is always first
                for line in f:
                    cols = line.strip().split("\t")
                    if len(cols) > type_idx and cols[type_idx] == "ref":
                        existing_ref_gcas.add(cols[gca_idx])

    # 加载 Taxonomy (TaxID -> Rank Info)
    tax_info = {}
    if taxonomy_dir:
        tax_summary = taxonomy_dir / "ncbi_dataset/data/taxonomy_summary.tsv"
        if tax_summary.exists():
            with tax_summary.open("r") as f:
                header = f.readline()
                for line in f:
                    cols = line.strip().split("\t")
                    tid = cols[1]
                    ranks = "|".join(cols[10:24]) # 假设后面是 ranks
                    tax_info[tid] = ranks

    # 合并
    # 读取 clean_tsv (GCA, Name, Len, TaxID)
    records = []
    with clean_tsv_file.open("r") as f:
        for line in f:
            cols = line.strip().split("\t")
            taxid = cols[3]
            extra = tax_info.get(taxid, "NA")
            records.append(cols + [extra])
            
    # 排序并写入 (按长度降序)
    records.sort(key=lambda x: int(x[2]), reverse=True)
    
    with tmp.open("w") as f:
        f.write("GCA\tOrganism\tLength\tTaxID\tLineage\tType\n")
        for rec in records:
            # rec: [GCA, Organism, Length, TaxID, Lineage]
            gca = rec[0]
            curr_type = genome_type
            if genome_type == "all" and gca in existing_ref_gcas:
                curr_type = "ref"
            
            f.write("\t".join(rec) + f"\t{curr_type}\n")
    tmp.replace(target_meta)
    Logger.success(f"元数据更新完成: {target_meta}")

def check_gz_integrity(gz_path, chunk_size=1024 * 1024):
    #Logger.info("检查压缩文件是否完整中...", end="")
    try:
        with gzip.open(gz_path, "rb") as f:
            while f.read(chunk_size):
                pass
        return True
    except Exception as e:
        Logger.warning(f"{gz_path.name} 文件损坏：{e}")
        return False

def parse_args():
    parser = argparse.ArgumentParser(description='NCBI Genome Batch Downloader & Updater')
    parser.add_argument('--taxon', type=str, required=True, help="Taxon name (e.g., fungi, bacteria)")
    parser.add_argument('--genome_dir', type=str, required=True, help="Directory to store genomic data")
    parser.add_argument("--genome_type", default="ref", choices=["ref", "all"], help="Genome category (ref/all)")
    parser.add_argument("--overwrite", action="store_true", help="Force overwrite existing downloads")
    parser.add_argument('--threads', type=int, default=4, help="Parallel threads (default: 4)")
    parser.add_argument('--tmp_dir', type=str, help='Temporary directory')
    parser.add_argument('--batch_size', type=int, default=500, help="Max TaxIDs per batch for taxonomy download (default: 500)")
    parser.add_argument('--parallel_downloads', type=int, default=4, help="Max parallel taxonomy batch downloads (default: 4)")
    parser.add_argument('--api_key', type=str, default=None, help="NCBI API key for datasets commands")
    return parser.parse_args()

def main():
    args = parse_args()
    
    taxon = args.taxon
    genome_root = Path(args.genome_dir).resolve()
    tmp_base = Path(args.tmp_dir or os.getcwd()).resolve()
    
    # 检查工作目录匹配
    if taxon.lower() not in genome_root.name.lower():
        Logger.warning(f"目标目录 '{genome_root.name}' 似乎不包含 taxon 名 '{taxon}'")
        if input("Continue? [y/N] ").lower() != 'y': sys.exit(0)

    # 设置目录
    work_tmp_dir = tmp_base / taxon
    work_tmp_dir.mkdir(parents=True, exist_ok=True)
    
    genome_root.mkdir(parents=True, exist_ok=True)

    Logger.step(f"Task: Update {taxon} Genome (Type: {args.genome_type})")

    # 1. 获取元数据
    meta_json = work_tmp_dir / f"{taxon}.jsonl"
    meta_tsv = work_tmp_dir / f"{taxon}.tsv"
    meta_clean = work_tmp_dir / f"{taxon}.clean.tsv"
    
    fetch_genome_summary(taxon, args.genome_type, meta_json, args.overwrite, args.api_key)
    convert_jsonl_to_tsv(meta_json, meta_tsv)
    count = parse_and_clean_metadata(meta_tsv, meta_clean)
    if count == 0:
        Logger.error("未找到有效的基因组记录，退出。")
        sys.exit(1)

    # 2. 检查更新
    list_file, taxid_file, deprecated_file = check_updates_and_plan(meta_clean, genome_root)
    print(list_file,)
    if list_file:
        # 3. 下载
        Logger.step("Downloading Data")
        tax_report_dir = download_taxonomy_info(taxid_file, args.overwrite, args.batch_size, args.api_key, args.parallel_downloads)
        raw_data_dir = download_genomes(list_file, args.overwrite, args.api_key)
        
        # 4. 处理
        Logger.step("Processing Genomes")
        processed_dir = validate_and_process_genomes(raw_data_dir, list_file, args.threads)
        
        # 5. 更新仓库
        Logger.step("Updating Repository")
        update_repository(processed_dir, genome_root, deprecated_file)
        
        # 6. 更新元数据表
        update_metadata_table(meta_clean, tax_report_dir, genome_root, args.genome_type)
        
        Logger.step("ALL DONE!", level="SUCCESS")
    elif deprecated_file:
        # 更新仓库
        Logger.step("Updating Repository")
        update_repository(work_tmp_dir, genome_root, deprecated_file)

        Logger.step("ALL DONE!", level="SUCCESS")
    else:
        Logger.success("No updates required.")

if __name__ == "__main__":
    main()

