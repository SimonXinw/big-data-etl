# Shopify 拉数、清洗分层与 BI 查看（端到端说明）

本文回答三个问题：**数据从哪里拉**、**清洗与 Shopify 字段如何对齐**、**报表在哪里看**。并与默认 `python -m etl_project`（`--source csv`）的区别写清楚。

## 1. 为什么终端里看不到 Shopify？

你看到的输出是：

```text
数据来源: csv
```

说明本次跑的是 **默认数据源 `csv`**：直接读仓库里已有的 `data/raw/*.csv`，**不会**访问 Shopify API。

Shopify 已接入代码库，但属于 **显式开关**，只有加上 `--source shopify` 时才会执行拉数逻辑。

对应代码（编排顺序）在 `etl_project/pipeline.py`：

1. 若 `data_source == "shopify"` → 先调用 `sync_shopify_to_csv()`，把 Admin GraphQL 结果写成 `data/raw` 下与示例一致的 CSV。
2. 若 `data_source == "inbox"` → 先 `materialize_raw_from_inbox()`。
3. 然后三条源（csv / 上两步之后的 raw）**共用同一套后续步骤**：校验源文件 → DuckDB `load_raw_tables` → `run_transformations`（raw→stg→dw→mart）→ 质量检查 → 可选导出 → 可选发布 Postgres。

因此：**Shopify 只影响「第 0 步：把远程数据变成 raw CSV」**；清洗与分层始终是 DuckDB 里的 SQL（`load.py` / `transform.py`），与是否来自 Shopify 无关。

## 2. 三步分别是什么、怎么执行？

### 第一步：数据拉取（Shopify → `data/raw`）

**做什么**：通过 `lib.shopify` 调 Shopify **Admin GraphQL**（只读），分页拉客户与订单，由 `etl_project/shopify_sync.py` 写成项目约定的 **`customers.csv` / `orders.csv`**（路径在 `etl_project/config.py` 的 `ProjectPaths` 中，一般为 `data/raw/`）。

**怎么执行**：

1. 在项目根目录准备 **`.env`**（可复制 `.env.example`），至少配置：

   ```env
   SHOPIFY_STORE_DOMAIN=your-store.myshopify.com
   SHOPIFY_ADMIN_ACCESS_TOKEN=shpat_xxx
   ```

   可选：`SHOPIFY_API_VERSION`（见 `.env.example` 注释）。

2. 运行（拉数 + 整链 ETL 一次完成）：

   ```powershell
   python -m etl_project --source shopify --target duckdb
   ```

成功时，终端除「ETL 运行完成」外，还会出现类似：

```text
Shopify 同步: customers=<n>, orders=<m>
```

若仍没有这行，说明没有走到 `shopify` 分支（例如仍是默认 `csv`）。

**注意**：需要有效店铺与自定义应用 Access Token，且本机网络能访问 Shopify API。

---

### 第二步：数据清洗与「和 Shopify 对齐」

这里分两层含义，避免混淆：

| 层次 | 做什么 | 代码位置 |
|------|--------|----------|
| **字段对齐（源 → raw）** | 把 GraphQL 的节点字段映射成与本地示例 CSV **相同的列名与类型约定**，保证后面 SQL 不用分叉。游客订单等规则也在此处理。 | `etl_project/shopify_sync.py`（注释写明：只负责「源系统 JSON → 统一 raw 列」） |
| **清洗与建模（raw → mart）** | 去重、标准化、维表事实表、指标聚合、质量规则。 | `etl_project/load.py`、`etl_project/transform.py`、`etl_project/quality.py` |

也就是说：**「和 Shopify 匹配」主要体现在 `shopify_sync` 把数据变成项目认的 raw 形态**；真正的「清洗」是 DuckDB 里统一的那套分层 SQL，**csv / inbox / shopify 三条源共用**。

执行上**不需要单独第二条命令**：`--source shopify --target duckdb` 已包含拉数 + load + transform + 质量检查 + 导出（除非加 `--skip-export`）。

---

### 第三步：BI 报表生成与查看

本仓库的推荐路径在 `docs/bi-guide.md`：**不把 Metabase 直接绑在 DuckDB 教学路径上**，而是：

```text
Python ETL（DuckDB 内计算） → 发布到 PostgreSQL / Supabase → Metabase 连库做仪表盘
```

**（A）只想快速看数（无 BI 工具）**

- 看导出：`data/output/sales_summary.csv`
- 看质量：`data/output/quality_report.json`
- 需要自助查表：用任意支持 DuckDB 的客户端打开 `data/warehouse/etl_learning.duckdb`，查询 `mart_*` 等表（表名以 `transform.py` 为准）。

**（B）按项目推荐方式看 BI（Metabase）**

1. **初始化分析库表结构**（在 Postgres 或 Supabase SQL 编辑器执行一次）：

   - 仓库内脚本：`sql/postgres/init_warehouse.sql`

2. **配置连接串**：根目录 `.env` 中设置 `ETL_POSTGRES_DSN`（见 `.env.example`）。

3. **跑 ETL 并发布到 Postgres**（Shopify 源示例）：

   ```powershell
   python -m etl_project --source shopify --target postgres
   ```

4. **启动 Metabase**（需本机已安装 Docker）：

   ```powershell
   cd bi\metabase
   docker compose up -d
   ```

   浏览器访问：`http://localhost:3000`，按向导完成首次设置。

5. **在 Metabase 里「添加数据库」**：类型选 PostgreSQL，填写与 `ETL_POSTGRES_DSN` 一致的连接信息。

6. **建问题 / 仪表盘**：优先用 `mart` schema 下的表（如 `mart.daily_sales`、`mart.city_sales` 等），具体图表与口径见 `docs/metabase-dashboard-template.md`。

若 Metabase 与 Postgres 不在同一台机器，注意防火墙与连接串中的 host 要互相可达。

## 3. 命令速查

| 目标 | 命令 |
|------|------|
| 本地示例 CSV，仅 DuckDB | `python -m etl_project` 或 `--source csv --target duckdb` |
| Shopify 拉数 + DuckDB | `python -m etl_project --source shopify --target duckdb`（需 `.env` 中 Shopify 变量） |
| Shopify 拉数 + 发布 Postgres 给 BI | `python -m etl_project --source shopify --target postgres`（需 `ETL_POSTGRES_DSN` + 已执行 `init_warehouse.sql`） |

## 4. 与 Airflow 的关系

示例 DAG `airflow/dags/python_etl_dag.py` 默认是：

```text
--source "${ETL_DATA_SOURCE:-csv}"
```

要调度 Shopify，需在 Airflow 侧把环境变量 `ETL_DATA_SOURCE` 设为 `shopify`，并安全注入 Shopify 与（若需要）`ETL_POSTGRES_DSN` 等凭据，而不是写死在 DAG 文件里。

## 5. 延伸阅读

- 项目总览：`README.md`（含 Shopify 小节与增量说明）
- 模块职责：`docs/project-structure.md`
- BI 选型与 Metabase：`docs/bi-guide.md`、`docs/metabase-dashboard-template.md`
- 本地不涉及 Shopify 的最短步骤：`docs/local-etl-quickstart.md`
