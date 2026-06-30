#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
自定义异常层次结构。

将原脚本中的 sys.exit() 替换为可捕获的异常，
使各模块可作为库被 genome_manager.tasks 等上层模块安全调用。

异常层次
--------
DownloaderError               # 所有异常的基类
├── DependencyError           # 缺少必要工具，无法继续
├── CommandError              # 子进程命令执行失败
├── DownloadError             # 网络下载失败（含批次重试耗尽）
├── ProcessingError           # 基因组处理阶段失败
└── NoUpdatesNeeded           # 本地已是最新，无需操作（非错误，信息性）
"""


class DownloaderError(Exception):
    """所有下载器异常的基类。"""


class DependencyError(DownloaderError):
    """依赖工具缺失或无法自动安装。"""


class CommandError(DownloaderError):
    """子进程命令以非零退出码结束。"""

    def __init__(
        self,
        cmd: str,
        returncode: int,
        stdout: str = "",
        stderr: str = "",
    ) -> None:
        self.cmd        = cmd
        self.returncode = returncode
        self.stdout     = stdout
        self.stderr     = stderr
        super().__init__(
            f"Command failed (returncode={returncode}): {cmd}"
        )


class DownloadError(DownloaderError):
    """网络下载失败，包括重试耗尽的情况。"""


class ProcessingError(DownloaderError):
    """基因组数据处理阶段（解压 / MD5 / BLAST）失败。"""


class NoUpdatesNeeded(DownloaderError):
    """本地数据已是最新，无需任何操作。

    这是一个信息性异常（非错误），CLI 层应以退出码 0 处理。
    """
