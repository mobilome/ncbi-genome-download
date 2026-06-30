#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
依赖检查与自动安装模块。

检查 NCBI CLI 工具（datasets / dataformat）是否可用，
不存在时自动从 NCBI FTP 下载并存放到工具目录。
其他系统工具（curl/unzip/makeblastdb/samtools）仅警告。

工具目录优先级（从高到低）:
  1. 环境变量 NCBI_TOOLS_DIR（可自定义）
  2. APP_DIR/bins（默认，已挂载到宿主机）
  3. 项目根目录下 bins/

删除工具目录中的可执行文件可触发自动重新下载；
直接替换该文件即可升级版本。

Raises:
    DependencyError: 必要工具缺失且无法自动安装。
"""
from __future__ import annotations

import os
import platform
import re
import shutil
import subprocess
import tarfile as _tarfile
import tempfile
from pathlib import Path

from .exceptions import DependencyError
from .logger import Logger


def _tools_dir() -> Path:
    """返回工具存放目录。优先使用 NCBI_TOOLS_DIR，其次 APP_DIR/bins。"""
    custom = os.environ.get("NCBI_TOOLS_DIR", "").strip()
    if custom:
        p = Path(custom)
    else:
        app_dir = os.environ.get("APP_DIR", "").strip()
        p = Path(app_dir) / "bins" if app_dir else Path(__file__).parent.parent / "bins"
    p.mkdir(parents=True, exist_ok=True)
    return p


_BLAST_BASE_URL = "https://ftp.ncbi.nlm.nih.gov/blast/executables/blast+/LATEST"


def _blast_tarball_pattern() -> str | None:
    """返回当前平台对应的 BLAST+ 压缩包文件名正则，不支持则返回 None。"""
    system  = platform.system().lower()
    machine = platform.machine().lower()
    if system == "linux":
        if machine in ("aarch64", "arm64") or machine.startswith("arm"):
            return None  # NCBI 暂无 Linux arm 预编译包
        return r"ncbi-blast-[\d.+]+-x64-linux\.tar\.gz"
    if system == "darwin":
        return r"ncbi-blast-[\d.+]+-x64-macosx\.tar\.gz"
    return None


def _install_blast_tools(tools_dir: Path, missing: list[str]) -> None:
    """从 NCBI FTP 下载最新 BLAST+ 压缩包并提取指定工具到 tools_dir。

    一次下载，同时提取所有 missing 列表中的工具，避免重复下载。
    """
    pattern = _blast_tarball_pattern()
    if pattern is None:
        Logger.warning(
            "当前平台无 NCBI BLAST+ 预编译包，请手动安装以下工具: "
            + ", ".join(missing) + "\n"
            "参考: https://www.ncbi.nlm.nih.gov/books/NBK569861/"
        )
        return

    if not shutil.which("curl"):
        Logger.warning("未找到 'curl'，无法自动下载 BLAST+ 工具，请手动安装: " + ", ".join(missing))
        return

    # 获取 LATEST 目录的文件列表，找到压缩包名
    res = subprocess.run(
        ["curl", "-fsSL", f"{_BLAST_BASE_URL}/"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    if res.returncode != 0:
        Logger.warning(
            f"获取 BLAST+ 文件列表失败，请手动安装: {', '.join(missing)}\n"
            + res.stderr.decode().strip()
        )
        return

    m = re.search(pattern, res.stdout.decode())
    if not m:
        Logger.warning("无法在 NCBI FTP 找到 BLAST+ 压缩包，请手动安装: " + ", ".join(missing))
        return

    tarball_name = m.group(0)
    url = f"{_BLAST_BASE_URL}/{tarball_name}"
    Logger.warning(f"下载 {tarball_name} 以安装: {', '.join(missing)} ...")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_tar = Path(tmpdir) / tarball_name
        res = subprocess.run(
            ["curl", "-fsSL", "-o", str(tmp_tar), url],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        if res.returncode != 0:
            Logger.warning(
                f"下载 BLAST+ 失败，请手动安装: {', '.join(missing)}\n"
                + res.stderr.decode().strip()
            )
            return

        remaining = set(missing)
        try:
            with _tarfile.open(tmp_tar) as tf:
                for member in tf.getmembers():
                    basename = member.name.rsplit("/", 1)[-1]
                    if basename in remaining and member.isfile():
                        f_obj = tf.extractfile(member)
                        if f_obj is None:
                            Logger.warning(f"无法读取 {basename} 内容，跳过。")
                            continue
                        target = tools_dir / basename
                        target.write_bytes(f_obj.read())
                        target.chmod(target.stat().st_mode | 0o755)
                        Logger.success(f"'{basename}' 已下载到 {target}")
                        remaining.discard(basename)
                        if not remaining:
                            break
        except Exception as exc:
            Logger.warning(f"解压 BLAST+ 失败: {exc}，请手动安装: {', '.join(missing)}")
            return

        if remaining:
            Logger.warning("BLAST+ 压缩包中未找到以下工具，请手动安装: " + ", ".join(sorted(remaining)))


def check_and_install_dependencies() -> None:
    """检查并自动安装必要的软件依赖。

    Raises:
        DependencyError: 工具下载失败或当前平台不支持自动安装。
    """
    tools_dir = _tools_dir()

    # 确保工具目录在 PATH 前端（本进程内生效）
    path_env = os.environ.get("PATH", "")
    if str(tools_dir) not in path_env.split(os.pathsep):
        os.environ["PATH"] = str(tools_dir) + os.pathsep + path_env

    # ── 确定平台下载 URL ──────────────────────────────────────────────────────
    system  = platform.system().lower()
    machine = platform.machine().lower()
    base_url: str | None

    if system == "linux":
        if machine in ("aarch64", "arm64"):
            base_url = "https://ftp.ncbi.nlm.nih.gov/pub/datasets/command-line/v2/linux-arm64"
        elif machine.startswith("arm"):
            base_url = "https://ftp.ncbi.nlm.nih.gov/pub/datasets/command-line/v2/linux-arm"
        else:
            base_url = "https://ftp.ncbi.nlm.nih.gov/pub/datasets/command-line/v2/linux-amd64"
    elif system == "darwin":
        base_url = "https://ftp.ncbi.nlm.nih.gov/pub/datasets/command-line/v2/mac"
    else:
        Logger.warning(
            f"不支持自动安装的平台: {system}/{machine}，请手动安装 datasets/dataformat。"
        )
        Logger.warning(
            "安装说明: https://www.ncbi.nlm.nih.gov/datasets/docs/v2/command-line-tools/download-and-install/"
        )
        base_url = None

    # ── 检查并安装 NCBI CLI 工具 ──────────────────────────────────────────────
    for tool in ("datasets", "dataformat"):
        found = shutil.which(tool)
        if found:
            Logger.success(f"依赖检查通过: {tool} ({found})")
            continue

        if base_url is None:
            raise DependencyError(
                f"未找到 '{tool}'，当前平台不支持自动安装，请参考文档手动安装:\n"
                "https://www.ncbi.nlm.nih.gov/datasets/docs/v2/command-line-tools/download-and-install/"
            )

        if not shutil.which("curl"):
            raise DependencyError(
                f"未找到 'curl'，无法自动下载 '{tool}'，请手动安装。"
            )

        target = tools_dir / tool
        url    = f"{base_url}/{tool}"
        Logger.warning(f"未找到 '{tool}'，正在从 NCBI 下载到 {target}: {url}")

        result = subprocess.run(
            ["curl", "-fsSL", "-o", str(target), url],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if result.returncode != 0:
            raise DependencyError(
                f"下载 '{tool}' 失败 (returncode={result.returncode}):\n"
                f"{result.stderr.decode().strip()}\n"
                "请手动安装: https://www.ncbi.nlm.nih.gov/datasets/docs/v2/command-line-tools/download-and-install/"
            )

        target.chmod(target.stat().st_mode | 0o755)
        Logger.success(f"'{tool}' 已下载到 {target}")

    # ── makeblastdb + blastdbcmd：自动下载到工具目录 ─────────────────────────
    blast_tools = ("makeblastdb", "blastdbcmd")
    missing_blast = [t for t in blast_tools if not shutil.which(t)]
    found_blast   = [t for t in blast_tools if shutil.which(t)]
    for t in found_blast:
        Logger.success(f"依赖检查通过: {t} ({shutil.which(t)})")
    if missing_blast:
        _install_blast_tools(tools_dir, missing_blast)

    # ── 可选系统工具检查（仅警告）────────────────────────────────────────────
    optional_tools: dict[str, str] = {
        "curl":     "用于网络下载",
        "unzip":    "用于解压 zip 数据包",
        "samtools": "用于构建 FASTA 索引",
    }
    for tool, desc in optional_tools.items():
        if not shutil.which(tool):
            Logger.warning(f"未找到可选工具 '{tool}' ({desc})，相关步骤可能失败。")
