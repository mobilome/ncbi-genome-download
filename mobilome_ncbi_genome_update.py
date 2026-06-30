#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
向后兼容入口脚本。

实际逻辑已迁移至 genome_downloader/ 包，本文件仅作为薄封装保留兼容性。

原始用法（不变）::

    python mobilome_ncbi_genome_update.py --taxon fungi --genome_dir /data/fungi

直接调用新包（等效）::

    python -m genome_downloader --taxon fungi --genome_dir /data/fungi
"""

from genome_downloader.cli import main

if __name__ == "__main__":
    main()
