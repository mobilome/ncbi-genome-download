#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
check_genomes.py — 基因组文件完整性与 BLAST 数据库校验工具
============================================================

在 genome_dir 下扫描 non_ref/genomes/ 和 ref/genomes/ 子目录，执行：
  - integrity : 读取 *_md5sums.txt，逐一计算 .fna 文件 MD5 并比对
  - blast     : 对每个已建库的 BLAST 数据库执行 blastdbcmd -info 校验

用法
----
    python check_genomes.py --genome-dir /data/genomes/fungi --mode all --threads 8
    python check_genomes.py -d /data/genomes/fungi -m integrity -t 4
    python check_genomes.py -d /data/genomes/fungi -m blast -t 4 -v

返回值
------
    0 — 全部通过
    1 — 有失败 / 缺失
    2 — 参数错误或目录不存在
"""
from __future__ import annotations

import argparse
import hashlib
import os
import shutil
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# ── 确保能找到项目模块 ──────────────────────────────────────────────────────────
_project_root = Path(__file__).parent.resolve()
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

try:
    from genome_downloader.logger import Logger
    from genome_downloader.utils import (
        non_ref_genomes_dir,
        non_ref_md5_path,
        ref_genomes_dir,
        ref_md5_path,
    )
except ImportError:
    # 极少情况：运行目录不含项目模块，退回到简易 Logger
    import datetime
    class Logger:  # type: ignore[no-redef]
        COLORS = {"INFO": "\033[97m", "SUCCESS": "\033[92m",
                  "WARNING": "\033[93m", "ERROR": "\033[91m",
                  "STEP": "\033[95m"}
        RESET = "\033[0m"

        @classmethod
        def _log(cls, level: str, msg: str) -> None:
            now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"{cls.COLORS.get(level, '')}"
                  f"[{now}] [{level}] {msg}{cls.RESET}", flush=True)

        @classmethod
        def info(cls, m: str) -> None: cls._log("INFO", m)

        @classmethod
        def success(cls, m: str) -> None: cls._log("SUCCESS", m)

        @classmethod
        def warning(cls, m: str) -> None: cls._log("WARNING", m)

        @classmethod
        def error(cls, m: str) -> None: cls._log("ERROR", m)

        @classmethod
        def step(cls, m: str) -> None: cls._log("STEP", m)

    def non_ref_genomes_dir(genome_dir: Path) -> Path:
        return genome_dir / "non_ref" / "genomes"

    def ref_genomes_dir(genome_dir: Path) -> Path:
        return genome_dir / "ref" / "genomes"

    def non_ref_md5_path(genome_dir: Path) -> Path:
        return genome_dir / "non_ref" / "md5sums.txt"

    def ref_md5_path(genome_dir: Path) -> Path:
        return genome_dir / "ref" / "md5sums.txt"


# ══════════════════════════════════════════════════════════════
# 内部辅助
# ══════════════════════════════════════════════════════════════

def _compute_md5(path: Path) -> str:
    hasher = hashlib.md5()
    with path.open("rb") as f:
        while chunk := f.read(1 << 20):
            hasher.update(chunk)
    return hasher.hexdigest()


def _find_blastdbcmd() -> str | None:
    """查找 blastdbcmd：优先 PATH，其次 $APP_DIR/bins/ 或项目 bins/。"""
    found = shutil.which("blastdbcmd")
    if found:
        return found
    app_dir = os.environ.get("APP_DIR", "").strip()
    bins_dir = Path(app_dir) / "bins" if app_dir else _project_root / "bins"
    candidate = bins_dir / "blastdbcmd"
    if candidate.exists():
        return str(candidate)
    return None


def _sep(title: str = "") -> None:
    line = "═" * 60
    if title:
        Logger.step(f"{line}")
        Logger.step(f"  {title}")
        Logger.step(f"{line}")
    else:
        Logger.step(line)


# ══════════════════════════════════════════════════════════════
# 1. 基因组文件完整性校验（MD5）
# ══════════════════════════════════════════════════════════════

def check_integrity(genome_dir: Path, threads: int = 4) -> int:
    """读取 *_md5sums.txt，逐一计算 .fna 文件 MD5 并比对。

    Returns:
        0 — 全部通过，1 — 发现问题
    """
    _sep("基因组文件完整性校验（MD5）")
    Logger.info(f"目录    : {genome_dir}")
    Logger.info(f"线程数  : {threads}")

    # ── 读取 md5sums 文件 ───────────────────────────────────────────────────
    entries: list[tuple[str, str, Path | None]] = []  # (gca, expected_md5, fna_or_None)
    for md5_file, sub_dir in [
        (non_ref_md5_path(genome_dir), non_ref_genomes_dir(genome_dir)),
        (ref_md5_path(genome_dir),     ref_genomes_dir(genome_dir)),
    ]:
        if not md5_file.exists():
            Logger.info(f"跳过（未找到 {md5_file.name}）")
            continue
        Logger.info(f"读取 {md5_file.name} …")
        count = 0
        for line in md5_file.open():
            parts = line.strip().split()
            if len(parts) == 2 and len(parts[1]) == 32:
                gca, expected = parts[0], parts[1]
                fna = sub_dir / f"{gca}.fna"
                entries.append((gca, expected, fna if fna.exists() else None))
                count += 1
        Logger.info(f"  → 读取 {count} 条记录")

    total = len(entries)
    if total == 0:
        Logger.warning("未找到任何 MD5 记录，请先完成下载步骤以生成 *_md5sums.txt。")
        _sep()
        return 1

    Logger.info(f"共 {total} 条记录，开始并行校验（{threads} 线程）…")

    # ── 并行校验 ────────────────────────────────────────────────────────────
    passed: int = 0
    failed: list[dict] = []
    missing: list[str] = []
    done: int = 0

    def _check_one(entry: tuple) -> tuple:
        gca, expected_md5, fna_path = entry
        if fna_path is None:
            return gca, "missing", None, None
        try:
            actual = _compute_md5(fna_path)
            return gca, ("ok" if actual == expected_md5 else "fail"), expected_md5, actual
        except OSError as exc:
            return gca, "missing", None, str(exc)

    with ThreadPoolExecutor(max_workers=threads, thread_name_prefix="md5chk") as pool:
        futures = {pool.submit(_check_one, e): e[0] for e in entries}
        for fut in as_completed(futures):
            gca, status, expected, actual = fut.result()
            done += 1
            pct = done * 100 // total

            if status == "ok":
                passed += 1
                Logger.info(f"[{done:>5}/{total} {pct:3d}%] ✓ {gca}")
            elif status == "fail":
                failed.append({"gca": gca, "expected": expected, "actual": actual})
                Logger.error(f"[{done:>5}/{total} {pct:3d}%] ✗ MD5 不符: {gca}")
                Logger.error(f"                      期望: {expected}")
                Logger.error(f"                      实际: {actual}")
            else:
                missing.append(gca)
                Logger.warning(f"[{done:>5}/{total} {pct:3d}%] ? 文件缺失: {gca}")

    # ── 汇总 ────────────────────────────────────────────────────────────────
    _sep("完整性校验汇总")
    Logger.info(f"  总计        : {total}")
    Logger.info(f"  MD5 通过    : {passed}")
    Logger.info(f"  MD5 不符    : {len(failed)}")
    Logger.info(f"  文件缺失    : {len(missing)}")

    if failed:
        Logger.error("─── MD5 校验失败的基因组 ───")
        for item in failed:
            Logger.error(f"  {item['gca']}")
            Logger.error(f"    期望: {item['expected']}")
            Logger.error(f"    实际: {item['actual']}")

    if missing:
        Logger.warning("─── 文件缺失的基因组 ───")
        for gca in missing:
            Logger.warning(f"  {gca}")

    ok = (len(failed) == 0 and len(missing) == 0)
    if ok:
        Logger.success("✓ 所有基因组文件完整性校验通过。")
    else:
        Logger.error(f"✗ 发现 {len(failed) + len(missing)} 个问题。")
    _sep()
    return 0 if ok else 1


# ══════════════════════════════════════════════════════════════
# 2. BLAST 数据库校验（blastdbcmd -info）
# ══════════════════════════════════════════════════════════════

def check_blast_db(genome_dir: Path, threads: int = 4) -> int:
    """对每个已建库的 BLAST 数据库执行 blastdbcmd -info 校验。

    Returns:
        0 — 全部通过，1 — 发现问题
    """
    _sep("BLAST 数据库完整性校验（blastdbcmd -info）")
    Logger.info(f"目录    : {genome_dir}")
    Logger.info(f"线程数  : {threads}")

    blastdbcmd = _find_blastdbcmd()
    if not blastdbcmd:
        Logger.error("找不到 blastdbcmd！请确认 BLAST+ 已安装（PATH 或 bins/ 目录）。")
        _sep()
        return 1
    Logger.info(f"使用    : {blastdbcmd}")

    # ── 扫描已建库的基因组 ──────────────────────────────────────────────────
    to_check: list[tuple[Path, Path]] = []  # (fna, db_prefix)
    for scan_dir in (non_ref_genomes_dir(genome_dir), ref_genomes_dir(genome_dir)):
        if not scan_dir.exists():
            continue
        fna_list = sorted(scan_dir.glob("*.fna"))
        built = [(fna, fna.with_suffix(""))
                 for fna in fna_list
                 if (fna.parent / (fna.stem + ".nhr")).exists()]
        Logger.info(f"扫描 {scan_dir.name}/: {len(fna_list)} 个 .fna，"
                    f"其中 {len(built)} 个已建库")
        to_check.extend(built)

    total = len(to_check)
    if total == 0:
        Logger.warning("未找到已建库的 BLAST 数据库，请先完成「格式化基因组」步骤。")
        _sep()
        return 1

    Logger.info(f"共 {total} 个数据库，开始并行校验（{threads} 线程）…")

    # ── 并行校验 ────────────────────────────────────────────────────────────
    failed: list[dict] = []
    done: int = 0

    def _validate_one(fna: Path, db_prefix: Path) -> tuple[str, bool, str]:
        gca = fna.stem
        try:
            result = subprocess.run(
                [blastdbcmd, "-db", str(db_prefix), "-info"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
            if result.returncode == 0:
                return gca, True, ""
            return gca, False, result.stderr.decode(errors="replace").strip()
        except FileNotFoundError:
            return gca, False, "blastdbcmd: 命令未找到"

    with ThreadPoolExecutor(max_workers=threads, thread_name_prefix="blastchk") as pool:
        futures = {pool.submit(_validate_one, fna, dp): fna.stem
                   for fna, dp in to_check}
        for fut in as_completed(futures):
            gca, ok, err = fut.result()
            done += 1
            pct = done * 100 // total

            if ok:
                Logger.info(f"[{done:>5}/{total} {pct:3d}%] ✓ {gca}")
            else:
                failed.append({"gca": gca, "error": err})
                Logger.error(f"[{done:>5}/{total} {pct:3d}%] ✗ {gca}")
                if err:
                    # blastdbcmd 的错误信息可能多行，每行缩进显示
                    for line in err.splitlines():
                        Logger.error(f"                      {line}")

    # ── 汇总 ────────────────────────────────────────────────────────────────
    _sep("BLAST 数据库校验汇总")
    Logger.info(f"  总计        : {total}")
    Logger.info(f"  校验通过    : {total - len(failed)}")
    Logger.info(f"  校验失败    : {len(failed)}")

    if failed:
        Logger.error("─── 校验失败的数据库 ───")
        for item in failed:
            Logger.error(f"  {item['gca']}")
            if item["error"]:
                for line in item["error"].splitlines():
                    Logger.error(f"    {line}")

    ok = (len(failed) == 0)
    if ok:
        Logger.success("✓ 所有 BLAST 数据库校验通过。")
    else:
        Logger.error(f"✗ {len(failed)} 个数据库校验失败。")
    _sep()
    return 0 if ok else 1


# ══════════════════════════════════════════════════════════════
# 入口
# ══════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        prog="check_genomes.py",
        description="基因组文件完整性（MD5）与 BLAST 数据库（blastdbcmd）校验工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
示例：
  python check_genomes.py -d /data/genomes/fungi --mode all -t 8
  python check_genomes.py -d /data/genomes/fungi --mode integrity
  python check_genomes.py -d /data/genomes/fungi --mode blast -t 4
""",
    )
    parser.add_argument(
        "--genome-dir", "-d",
        required=True,
        metavar="DIR",
        help="基因组目录（含 non_ref/genomes/ 和/或 ref/genomes/ 子目录）",
    )
    parser.add_argument(
        "--mode", "-m",
        choices=["integrity", "blast", "all"],
        default="all",
        help="校验模式：integrity（MD5）| blast（BLAST 数据库）| all（两者，默认）",
    )
    parser.add_argument(
        "--threads", "-t",
        type=int,
        default=4,
        metavar="N",
        help="并行线程数（默认 4）",
    )

    args = parser.parse_args()

    genome_dir = Path(args.genome_dir).resolve()
    if not genome_dir.exists():
        Logger.error(f"目录不存在: {genome_dir}")
        sys.exit(2)
    if not genome_dir.is_dir():
        Logger.error(f"路径不是目录: {genome_dir}")
        sys.exit(2)


    exit_codes: list[int] = []

    if args.mode in ("integrity", "all"):
        exit_codes.append(check_integrity(genome_dir, args.threads))

    if args.mode in ("blast", "all"):
        exit_codes.append(check_blast_db(genome_dir, args.threads))

    sys.exit(max(exit_codes) if exit_codes else 0)


if __name__ == "__main__":
    main()
