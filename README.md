# NCBI 基因组下载与更新

简介
- 本仓库用于批量下载、校验与维护 NCBI 真核基因组（按分类组织，例如 `fungi`、`metazoa`、`viridiplantae` 等）。
- 包含获取 accession 列表、dehydrated 下载、MD5 校验、识别更新/废弃基因组与批量格式化（如 `makeblastdb`）等流程示例。

主要脚本
- `mobilome_ncbi_genome_update.py`：核心的更新/检查脚本（请使用 `--help` 查看完整参数）。

先决条件（示例）
- NCBI 命令行：`ncbi-datasets`、`dataformat`（可从 NCBI 官方下载二进制）
- 常用工具：`awk`, `sed`, `parallel`, `gzip`, `unzip`, `md5sum`, `coreutils`
- 生物信息工具：`makeblastdb`（BLAST+）、`seqkit`、`samtools`

安装（示例）

1) 使用包管理器（Debian/Ubuntu，可能已包含 `ncbi-datasets-cli`）：

```bash
sudo apt update
sudo apt install ncbi-datasets-cli parallel unzip pigz seqkit samtools ncbi-blast+ coreutils
```

2) 或直接从 NCBI 官方下载 `dataset` 与 `dataformat` 二进制（Linux x86_64 示例）：

```bash
curl -LO https://ftp.ncbi.nlm.nih.gov/pub/datasets/command-line/LATEST/linux-amd64/datasets
curl -LO https://ftp.ncbi.nlm.nih.gov/pub/datasets/command-line/LATEST/linux-amd64/dataformat
chmod +x datasets dataformat
sudo mv datasets dataformat /usr/local/bin/
```

快速开始

使用仓库脚本检查并更新（示例）：

```bash
python3 mobilome_ncbi_genome_update.py --genome_dir /path/to/GCA --tmp_dir ./tmpdir --threads 4 --taxon fungi
```

脚本用法（详细）

参数说明：
- `--taxon` (必需)：分类名称，例如 `fungi`，用于查询和临时目录命名。
- `--genome_dir` (必需)：存放基因组与索引的根目录（脚本会在此目录下创建 `GCA/`、`deprecated/` 等）。
- `--genome_type`：`ref` 或 `all`，默认为 `ref`，决定 `datasets summary` 是否带 `--reference`。
- `--overwrite`：存在下载包或中间文件时强制覆盖（默认不覆盖）。
- `--threads`：并行线程数，默认 `4`，影响解压、md5 与 makeblastdb 并发数。
- `--tmp_dir`：可选临时目录，默认使用当前工作目录。

脚本执行流程（主要步骤）：
1. 获取元数据：调用 `datasets summary genome`（输出 `{taxon}.jsonl`）。
2. 转换并清洗：`dataformat` -> TSV -> 清洗为 `{taxon}.clean.tsv`。
3. 生成更新计划：比较本地索引（`gca_file_index.tsv`）与远程，产生：
	- `{taxon}.genome_to_download`（待下载的 GCA 列表）
	- `{taxon}.taxon_to_download`（涉及的 TaxID 列表）
	- `{taxon}.deprecated`（废弃 GCA 列表）
4. 下载数据：使用 `datasets download --dehydrated` 获取包并 `rehydrate`（生成 `genome_download_tmp` 下的 `data/`）。
5. 处理与校验：并行解压、计算 md5、调用 `makeblastdb` 与 `samtools faidx`，在 `processing/` 目录中生成中间结果并行执行。
6. 更新仓库：将处理完成的 `.fna` 与 blastdb 移动到 `GCA/`，将废弃项移入 `deprecated/`，并重建 `gca_file_index.tsv` 与 `genome_metadata.tsv`。

重要提醒：
- 脚本会在开始时检查 `--taxon` 与 `--genome_dir` 名称是否匹配，若不匹配会提示并要求确认。
- 下载与并行解压会产生大量临时数据，请确保目标文件系统有足够空间并注意 I/O 限制。


Others（按分类筛选示例）

```bash
# 从已有分类列表中排除以得到 eukaryota-others
cat fungi.gca_accession.txt metazoa.gca_accession.txt viridiplantae.gca_accession.txt > exclude.gca_accession.txt
awk 'NR==FNR{a[$0]; next} !($0 in a)' exclude.gca_accession.txt eukaryota.gca_accession.txt > eukaryota-others.gca_accession.txt

# metazoa-others 示例
cat arthropoda.gca_accession.txt chordata.gca_accession.txt > exclude.gca_accession.txt
awk 'NR==FNR{a[$0]; next} !($0 in a)' exclude.gca_accession.txt metazoa.gca_accession.txt > metazoa-others.gca_accession.txt

# chordata-others 示例
cat actinopteri.gca_accession.txt aves.gca_accession.txt mammalia.gca_accession.txt > exclude.gca_accession.txt
awk 'NR==FNR{a[$0]; next} !($0 in a)' exclude.gca_accession.txt chordata.gca_accession.txt > chordata-others.gca_accession.txt
```
