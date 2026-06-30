#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
通用工具函数。

包含：
- GCA_PATTERN   : 全局 GCA 编号正则
- run_cmd()     : 捕获输出的命令执行（失败抛 CommandError）
- run_shell()   : 实时输出的命令执行（直接接管 stdout/stderr）
- check_gz_integrity() : 验证 .gz 文件完整性
"""
from __future__ import annotations

import gzip
import os
import re
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

from .exceptions import CommandError
from .logger import Logger

# 全局正则：匹配 GCA 编号，如 GCA_000001405.28
GCA_PATTERN = re.compile(r"(GCA_\d+\.\d+)")

# datasets CLI 进度条行
_PROGRESS_LINE_RE = re.compile(
    r"^(Collecting|Completed|Downloading:? |Validating package|Found \d+)"
)


def run_cmd(cmd: str, verbose: bool = True, shell: bool = True) -> str:
    """执行 Shell 命令并返回 stdout 字符串。

    失败时先打印错误信息，再抛出 :class:`CommandError`。
    上层可捕获该异常进行重试或汇报；CLI 层统一转换为 sys.exit。

    Args:
        cmd:     要执行的命令（字符串）。
        verbose: 是否将 stdout 打印到终端。
        shell:   是否通过 shell 执行。

    Returns:
        命令的 stdout 字符串（strip 后）。

    Raises:
        CommandError: 命令返回非零退出码。
    """
    try:
        result = subprocess.run(
            cmd,
            shell=shell,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if verbose and result.stdout:
            print(result.stdout.strip(), flush=True)
        return result.stdout.strip()

    except subprocess.CalledProcessError as e:
        Logger.error("Command failed!")
        Logger.error(f"Command: {cmd}")
        if e.stdout:
            print(f"\n[STDOUT]\n{e.stdout.strip()}", flush=True)
        if e.stderr:
            print(f"\n[STDERR]\n{e.stderr.strip()}", flush=True)
        raise CommandError(cmd, e.returncode, e.stdout or "", e.stderr or "") from e


def run_shell(cmd: str, quiet: bool = False, collapse_progress: bool = False) -> None:
    """实时执行 Shell 命令，stdout/stderr 直接写入当前进程终端，支持中断。

    此函数创建子进程执行命令。接收到 SIGTERM 信号时，会优雅地清理子进程：
    1. 向子进程发送 SIGTERM
    2. 等待最多 5 秒让子进程优雅退出
    3. 若超时，发送 SIGKILL 强制终止
    4. 清理后重新抛出原信号

    Args:
        cmd:               要执行的命令（字符串）。
        quiet:             是否抑制输出。True 则重定向到 /dev/null，False 则直接输出到终端。
        collapse_progress: 是否将进度条日志合并为单行显示（适用于 datasets CLI）。

    Raises:
        KeyboardInterrupt: 用户按下 Ctrl+C 或进程收到 SIGTERM。
    """
    process: subprocess.Popen | None = None
    sig_received: signal.Signals | None = None

    def signal_handler(signum: int, frame) -> None:
        """接收信号时，优雅关闭子进程。"""
        nonlocal sig_received
        sig_received = signal.Signals(signum)
        Logger.warning(f"接收到 {sig_received.name}，正在清理子进程...")
        if process is not None and process.poll() is None:
            try:
                # Step 1: SIGTERM
                os.kill(process.pid, signal.SIGTERM)
                # Step 2: 等待最多 5 秒
                start = time.time()
                while time.time() - start < 5:
                    if process.poll() is not None:
                        Logger.info("子进程已优雅关闭")
                        return
                    time.sleep(0.1)
                # Step 3: 超时，发送 SIGKILL
                Logger.warning("子进程未在 5 秒内关闭，发送 SIGKILL...")
                os.kill(process.pid, signal.SIGKILL)
                process.wait()
                Logger.warning("子进程已被强制终止")
            except ProcessLookupError:
                pass  # 进程已自动退出

    # 注册信号处理器（仅主线程支持；子线程中跳过以避免 ValueError）
    is_main = threading.current_thread() is threading.main_thread()
    old_sigterm = None
    old_sigint = None
    if is_main:
        old_sigterm = signal.signal(signal.SIGTERM, signal_handler)
        old_sigint = signal.signal(signal.SIGINT, signal_handler)

    try:
        if quiet:
            stdout = subprocess.DEVNULL
            stderr = subprocess.DEVNULL
            process = subprocess.Popen(
                cmd,
                shell=True,
                stdout=stdout,
                stderr=stderr,
                preexec_fn=lambda: signal.signal(signal.SIGTERM, signal.SIG_DFL),
            )
            process.wait()
            return

        if collapse_progress:
            process = subprocess.Popen(
                cmd,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                preexec_fn=lambda: signal.signal(signal.SIGTERM, signal.SIG_DFL),
            )
            assert process.stdout is not None
            last_progress = False
            for raw in process.stdout:
                line = raw.rstrip("\r\n").lstrip("\r")
                if not line:
                    continue
                if _PROGRESS_LINE_RE.match(line):
                    print(f"\r{line}", end="", flush=True)
                    last_progress = True
                else:
                    if last_progress:
                        print("", flush=True)
                        last_progress = False
                    print(line, flush=True)
            if last_progress:
                print("", flush=True)
            process.wait()
            return

        process = subprocess.Popen(
            cmd,
            shell=True,
            stdout=sys.stdout,
            stderr=sys.stderr,
            preexec_fn=lambda: signal.signal(signal.SIGTERM, signal.SIG_DFL),
        )
        process.wait()
    finally:
        # 恢复原信号处理器（仅主线程）
        if is_main and old_sigterm is not None:
            signal.signal(signal.SIGTERM, old_sigterm)
        if is_main and old_sigint is not None:
            signal.signal(signal.SIGINT, old_sigint)
        # 如果收到了信号，则重新抛出（让上层处理）
        if sig_received == signal.Signals.SIGTERM:
            raise KeyboardInterrupt("Task cancelled by SIGTERM")
        elif sig_received == signal.Signals.SIGINT:
            raise KeyboardInterrupt("Task cancelled by Ctrl+C")



def check_gz_integrity(gz_path: Path, chunk_size: int = 1024 * 1024) -> bool:
    """逐块读取 .gz 文件，验证其是否完整可解压。

    Args:
        gz_path:    .gz 文件路径。
        chunk_size: 每次读取的字节数。

    Returns:
        True 表示文件完整，False 表示文件损坏。
    """
    try:
        with gzip.open(gz_path, "rb") as f:
            while f.read(chunk_size):
                pass
        return True
    except Exception as e:
        Logger.warning(f"{gz_path.name} 文件损坏：{e}")
        return False


# ─────────────────────────────────────────────────────────────
# 布局常量与路径助手
# ─────────────────────────────────────────────────────────────

REF_DIR_NAME = "ref"
NON_REF_DIR_NAME = "non_ref"
GENOMES_DIR_NAME = "genomes"
BLASTDB_DIR_NAME = "blastdb"
MD5SUMS_FILE_NAME = "md5sums.txt"
METADATA_FILE_NAME = "genomes_metadata.tsv"


def ref_dir(genome_dir: Path) -> Path:
    return genome_dir / REF_DIR_NAME


def non_ref_dir(genome_dir: Path) -> Path:
    return genome_dir / NON_REF_DIR_NAME


def ref_genomes_dir(genome_dir: Path) -> Path:
    return ref_dir(genome_dir) / GENOMES_DIR_NAME


def non_ref_genomes_dir(genome_dir: Path) -> Path:
    return non_ref_dir(genome_dir) / GENOMES_DIR_NAME


def ref_blastdb_dir(genome_dir: Path) -> Path:
    return ref_dir(genome_dir) / BLASTDB_DIR_NAME


def non_ref_blastdb_dir(genome_dir: Path) -> Path:
    return non_ref_dir(genome_dir) / BLASTDB_DIR_NAME


def ref_md5_path(genome_dir: Path) -> Path:
    return ref_dir(genome_dir) / MD5SUMS_FILE_NAME


def non_ref_md5_path(genome_dir: Path) -> Path:
    return non_ref_dir(genome_dir) / MD5SUMS_FILE_NAME


def ref_metadata_path(genome_dir: Path) -> Path:
    return ref_dir(genome_dir) / METADATA_FILE_NAME


def non_ref_metadata_path(genome_dir: Path) -> Path:
    return non_ref_dir(genome_dir) / METADATA_FILE_NAME


def parse_ncbi_md5sum(md5sum_file: Path) -> dict[str, str]:
    """解析 NCBI datasets 提供的 md5sum.txt，返回 {GCA: md5hex} 映射。

    NCBI datasets 提供的 md5sum.txt 存储的是未压缩 .fna 文件的 MD5 值。
    路径格式示例：ncbi_dataset/data/GCA_xxx/GCA_xxx_genomic.fna
    """
    if not md5sum_file.exists():
        return {}
    md5_map: dict[str, str] = {}
    with md5sum_file.open() as f:
        for line in f:
            parts = line.strip().split(None, 1)
            if len(parts) < 2:
                continue
            hash_val = parts[0]
            filepath = parts[1].lstrip("*").strip()
            m = GCA_PATTERN.search(filepath)
            if not m:
                continue
            # 仅记录基因组序列文件的 MD5（.fna 或 .fna.gz）
            if not (filepath.endswith(".fna") or filepath.endswith(".fna.gz")):
                continue
            gca = m.group(1)
            # 优先使用未压缩 .fna 的 MD5（若存在），否则接受 .fna.gz
            if filepath.endswith(".fna") or gca not in md5_map:
                md5_map[gca] = hash_val
    return md5_map

