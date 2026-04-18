# Shopify 拉数、清洗分层与 BI 查看（端到端说明）

本文回答三个问题：**数据从哪里拉**、**清洗与 Shopify 字段如何对齐**、**报表在哪里看**。并与默认 `python -m etl_project`（`--source csv`）的区别写清楚。

## 1. 为什么终端里看不到 Shopify？

你看到的输出是：

```text
数据来源: csv
```

说明本次跑的是 **默认数据源 `csv`**：直接读仓库里已有的 `data/raw/*.csv`，**不会**访问 Shopify API。

Shopify 已接入代码库，但属于 **显式开关**，只有加上 `--source shopify` 时才会执行拉数逻辑。

对应代码（编排顺序）在 `etl_project/etl/pipeline.py`：

1. 若 `data_source == "shopify"` → 调用 `sync_shopify_orders_incremental()`（`etl_project/integrations/shopify/admin/wide_sync.py`）：按 UTC 自然日 + `updated_at` 窗口拉 **Admin GraphQL** 订单宽表，写入 DuckDB `raw_shopify_orders` 并衍生 `raw_orders` / `raw_customers`；可选 UPSERT 到 PostgreSQL `raw.shopify_orders`；可选把快照写入 `data/orders/*.json`。**不再**经过「先写 `data/raw` CSV」这条旧路径。
2. 若 `data_source == "inbox"` → 先 `materialize_raw_from_inbox()`。
3. `csv` / `inbox` 源：校验源文件 → DuckDB `load_raw_tables`。**shopify** 源跳过 CSV `load_raw_tables`**（宽表已写好 raw 表）**。之后三条源**共用**：`run_transformations`（raw→stg→dw→mart）→ 质量检查 → 可选导出 → 可选发布 Postgres。

因此：**Shopify 负责「Admin → 宽表 + 衍生 narrow raw」**；清洗与分层仍是 DuckDB 里同一套 SQL（`etl/transform.py`）。订单采集主链路为 `integrations/shopify/admin/wide_sync`（按 UTC **日历日**拆分 `updated_at` 窗口逐日拉取）。

## 2. 三步分别是什么、怎么执行？

### 第一步：数据拉取（Shopify Admin → DuckDB raw 表 / 可选 JSON）

**做什么**：通过 `lib.shopify` 调 **Admin GraphQL**（只读），按日与 bi-database 对齐的查询拉订单，由 `etl_project/integrations/shopify/admin/wide_sync.py` 映射为宽表行 → DuckDB `raw_shopify_orders`，再 SQL 衍生 `raw_customers` / `raw_orders`。可选：

- 将连接快照按日追加写入固定文件 **`data/orders/shopify_orders_snapshot.json`**（可用 **`ETL_SHOPIFY_JSON_SNAPSHOT_ENABLE=0`** 关闭）；
- 若配置 `ETL_POSTGRES_DSN`，对 **`raw.shopify_orders`** 做 UPSERT。

字段映射见 `etl_project/integrations/shopify/admin/order_mapping.py`；GraphQL 文档见 `lib/shopify/queries/orders.py`（`ORDERS_BY_UPDATED_DAY_TEMPLATE`），单日查询组装见 `etl_project/integrations/shopify/admin/orders_bi.py`。

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
Shopify 宽表 UPSERT 行数（批次累计）: <k>
```

若仍没有这些行，说明没有走到 `shopify` 分支（例如仍是默认 `csv`）。

**注意**：需要有效店铺与自定义应用 Access Token，且本机网络能访问 Shopify API。

---

### 第二步：数据清洗与「和 Shopify 对齐」

这里分两层含义，避免混淆：

| 层次 | 做什么 | 代码位置 |
|------|--------|----------|
| **字段对齐（源 → raw）** | Shopify：宽表字段与 bi-database 订单表对齐，再衍生 narrow `raw_*` 供学习用 transform。 | `integrations/shopify/admin/order_mapping.py`、`integrations/shopify/admin/wide_sync.py`。 |
| **清洗与建模（raw → mart）** | 去重、标准化、维表事实表、指标聚合、质量规则。 | `etl/load.py`、`etl/transform.py`、`etl/quality.py` |

也就是说：**「和 Shopify 匹配」主要体现在宽表映射 + 衍生 narrow raw**；真正的「清洗」是 DuckDB 里统一的分层 SQL，**csv / inbox / shopify 三条源共用**（shopify 的 narrow 表由宽表 SQL 生成，而非 CSV 文件）。

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
