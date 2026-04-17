# 本地整套流程执行清单（你要点哪些「文件 / 命令」）

下面按目标拆分：**只跑通本地 ETL（DuckDB）** 与 **连 BI 的完整链路**。说明里「执行」指：在终端跑命令、在数据库客户端跑 SQL、在浏览器点 Metabase，而不是去逐个 `python 某个子模块 .py`（日常不需要；统一入口是 `python -m etl_project`）。

## A. 只跑本地 ETL（最常见）

| 步骤 | 做什么 | 执行什么 |
|------|--------|----------|
| 1 | 装依赖（环境就绪后不必每次做） | 终端：`python -m pip install -r requirements.txt` 或 `.\scripts\dev\install_deps_dev.ps1` |
| 2 | 跑整条 Python ETL | 终端：`python -m etl_project`（默认 csv→duckdb）或 `.\scripts\dev\run_etl_csv_dev.ps1` |

可选数据源（三选一，都是**同一条** `pipeline`，只换「第一步原料」）：

| 数据从哪来 | 命令或脚本 |
|------------|------------|
| `data/raw/*.csv` | `python -m etl_project --source csv --target duckdb` / `run_etl_csv_dev.ps1` |
| `data/inbox` | `python -m etl_project --source inbox --target duckdb` / `run_etl_inbox_dev.ps1` |
| Shopify | `python -m etl_project --source shopify --target duckdb` / `run_etl_shopify_dev.ps1`（需 `.env`） |

**你不需要手动依次执行** `extract.py`、`load.py`、`transform.py` 等文件；`python -m etl_project` 内部会按顺序调用它们。

**产物路径**（由 `etl_project/config.py` 决定）：

- `data/warehouse/etl_learning.duckdb`
- `data/output/sales_summary.csv`
- `data/output/quality_report.json`

一键最短演示（依赖 + 默认 ETL）：`.\scripts\dev\run_local_duckdb_demo_dev.ps1`

---

## B. 本地 ETL + PostgreSQL + Metabase（完整「到报表」）

| 步骤 | 做什么 | 执行什么 |
|------|--------|----------|
| B1 | 初始化远端库表结构（**每个库做一次**） | 在 PostgreSQL / Supabase **SQL 客户端**中执行文件 **`sql/postgres/init_warehouse.sql`** 的全文（不是用 Python 跑该文件） |
| B2 | 配置连接 | 项目根目录 **`.env`**：`ETL_POSTGRES_DSN=...`（见 `.env.example`） |
| B3 | 跑 ETL 并发布 | `python -m etl_project --source csv --target postgres`（或 `inbox` / `shopify`）或 `.\scripts\dev\run_publish_postgres_dev.ps1` / `run_publish_postgres_dev.ps1 -Source inbox` |
| B4 | 启动 Metabase | 终端进入 `bi/metabase`：`docker compose up -d`，或 `.\scripts\dev\run_metabase_dev.ps1` |
| B5 | 看报表 | 浏览器打开 **`http://localhost:3000`**，在 Metabase 里添加数据库（填与 B2 相同的 Postgres），再按 **`docs/metabase-dashboard-template.md`** 建问题/仪表盘 |

**说明**：Airflow DAG（`airflow/dags/python_etl_dag.py`）是**调度可选件**；本地手工跑通流程时**不必**启动 Airflow。

---

## C. 关于「每个流程一个 `xxx_dev` 副本」

- **代码层面**：没有为每个阶段维护第二套 `*_dev.py`；阶段都在 `etl_project/pipeline.py` 里编排。
- **本仓库提供的「副本」**：`scripts/dev/` 下若干 **`*_dev.ps1`**，每个对应一种你常跑的本地命令组合，可直接执行。
- **若你希望连 DuckDB 文件名也隔离成 `*_dev`**：当前 `config.py` 未提供环境开关，需要自行改 `database_file` 或复制整个 `data/` 目录做实验；需要可作为后续小功能再加。

---

## D. 延伸阅读

- `docs/local-etl-quickstart.md`：本地 DuckDB 精简说明  
- `docs/shopify-and-bi-pipeline.md`：Shopify 与 BI 分工  
- `README.md`：全项目说明  
