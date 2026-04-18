# 本地 ETL 快速启动

本文说明如何在**本机**跑通「抽取 → raw → 分层转换 → 质量检查 → 导出」这条 Python ETL 流程，不涉及 PostgreSQL / Metabase（那些见仓库根目录 `README.md` 与 `docs/bi-guide.md`）。若关心 **Shopify 何时执行、如何拉数、如何做 BI**，请看 `docs/shopify-and-bi-pipeline.md`。

## 文档在哪

| 内容 | 路径 |
|------|------|
| 项目总览、数据源说明、增量与发布 | 根目录 `README.md` |
| 本地 ETL 一步清单（本文） | `docs/local-etl-quickstart.md` |
| **Shopify 拉数 → 清洗 → BI 端到端** | `docs/shopify-and-bi-pipeline.md` |
| **本地整套要跑哪些步骤 / 文件** | `docs/local-full-runbook.md` |
| **Windows 下一键 `*_dev` 脚本** | `scripts/dev/README.md` |
| 模块职责 | `docs/project-structure.md` |
| **`data/` 目录约定**（raw / products / orders 等） | `data/README.md` |
| BI 与 Metabase | `docs/bi-guide.md`、`docs/metabase-dashboard-template.md` |

## 环境要求

- **Python 3.10+**（建议与团队一致；未在仓库锁死小版本）
- 能执行 `python -m pip`

## 1. 进入项目根目录

项目根目录即包含 `requirements.txt`、`etl_project/` 的那一层目录。

## 2. 安装依赖

```powershell
python -m pip install -r requirements.txt
```

（可选）使用虚拟环境：

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

## 3. 启动本地 DuckDB ETL

### 方式 A：默认（读 `data/raw` 下 CSV）

```powershell
python -m etl_project
```

等价于：

```powershell
python -m etl_project --source csv --target duckdb
```

### 方式 B：从 `data/inbox` 物化再跑（本地中转）

先把 `data/inbox` 里的 JSON/CSV 规范成 `data/raw` 的 CSV，再走同一套分层 SQL：

```powershell
python -m etl_project --source inbox --target duckdb
```

仓库已自带示例 `data/inbox/customers.json`、`orders.json`，可直接运行上式联调。

### 方式 C：简单增量（按 `order_id` 追加订单）

在 DuckDB 目标上追加新订单、全量重算下游层：

```powershell
python -m etl_project --target duckdb --incremental
```

可与 `--source csv` 或 `inbox` 组合使用。

## 4. 运行后产物（本地）

| 产物 | 路径 |
|------|------|
| DuckDB 库文件 | `data/warehouse/etl_learning.duckdb` |
| 汇总导出 CSV | `data/output/sales_summary.csv` |
| 数据质量报告 | `data/output/quality_report.json` |

## 5. 自测

```powershell
python -m unittest discover -s tests -v
```

## 6. 可选：用 Airflow 调度同一命令

DAG 示例在 `airflow/dags/python_etl_dag.py`，实际执行的是：

```text
python -m etl_project --target "${ETL_TARGET:-duckdb}" --source "${ETL_DATA_SOURCE:-csv}"
```

需在 Airflow 环境中安装本仓库依赖，并设置工作目录或 `ETL_PROJECT_ROOT` 指向本项目根目录。详见 `.env.example` 中与 Airflow 相关的变量说明。

## 常见问题

**Q：需要 `.env` 吗？**  
纯本地 DuckDB 流程**不强制**。只有 `--target postgres` 或 Shopify 拉数等场景才需要按 `.env.example` 配置。

**Q：和 README 里「快速开始」有什么区别？**  
内容一致；本文把「只看本地」的路径单独收敛在一页，方便 onboarding。
