# 更新日志

本文档记录了 NCBI Genome Manager 项目的主要版本变更。

---

## [1.0.0] — 2024-xx-xx

### 新增
- **Web 管理后台**：基于 FastAPI 的可视化管理界面，支持任务创建、监控、日志查看
- **REST API**：完整的 RESTful API，支持任务管理、配置管理、基因组查询和统计
- **SSE 实时推送**：Server-Sent Events 实现任务状态实时更新
- **命令行下载器**：`genome_downloader` 包，支持独立命令行批量下载
- **NCBI 工具自动安装**：自动下载和配置 `datasets`、`dataformat`、`makeblastdb`、`blastdbcmd`
- **Taxonomy 分批并行下载**：支持大批量 Taxonomy 数据的高效下载
- **基因组 Dehydrated/Rehydrate 流程**：使用 NCBI datasets 工具的高效下载方式
- **MD5 完整性校验**：下载后自动校验文件完整性
- **BLAST 数据库构建**：自动调用 `makeblastdb` 构建本地 BLAST 数据库
- **自动更新调度器**：内置定时调度器，按配置自动检查和更新基因组数据
- **Docker 部署**：提供完整的 Docker 镜像构建和 docker-compose 部署方案
- **管理员认证**：基于 Session 的管理员登录认证系统
- **基因组完整性校验工具**：独立的 `check_genomes.py` 校验工具

### 技术栈
- **后端框架**：FastAPI 0.110+
- **数据库**：SQLite (aiosqlite)
- **前端**：原生 HTML/CSS/JavaScript
- **容器化**：Docker + Docker Compose
- **Python**：3.12+
