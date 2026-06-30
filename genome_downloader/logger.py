#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
终端彩色日志模块。

Logger 类直接映射原脚本的 Logger，保持输出格式不变。
"""
from __future__ import annotations

import datetime


class Logger:
    """带颜色的终端日志工具。"""

    COLORS = {
        "INFO":    "\033[97m",    # White
        "SUCCESS": "\033[92m",    # Green
        "WARNING": "\033[93m",    # Yellow
        "ERROR":   "\033[91m",    # Red
        "RUN":     "\033[96m",    # Cyan
        "SHELL":   "\033[94m",    # Blue
        "STEP":    "\033[95m",    # Magenta
    }
    RESET = "\033[0m"

    @classmethod
    def log(cls, level: str, msg: str) -> None:
        now   = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        color = cls.COLORS.get(level, "")
        print(f"{color}[{now}] [{level}] {msg}{cls.RESET}", flush=True)

    @classmethod
    def info(cls, msg: str) -> None:
        cls.log("INFO", msg)

    @classmethod
    def success(cls, msg: str) -> None:
        cls.log("SUCCESS", msg)

    @classmethod
    def warning(cls, msg: str) -> None:
        cls.log("WARNING", msg)

    @classmethod
    def error(cls, msg: str) -> None:
        cls.log("ERROR", msg)

    @classmethod
    def run(cls, msg: str) -> None:
        cls.log("RUN", msg)

    @classmethod
    def shell(cls, msg: str) -> None:
        cls.log("SHELL", msg)

    @classmethod
    def step(cls, title: str, level: str = "STEP", width: int = 70) -> None:
        print(f"\n{cls.COLORS.get(level, '')}{'=' * width}{cls.RESET}", flush=True)
        header  = f" {title} "
        pad_len = (width - len(header)) // 2
        sep     = "─" * max(0, pad_len)
        print(f"{cls.COLORS.get(level, '')}{sep}{header}{sep}{cls.RESET}\n", flush=True)
