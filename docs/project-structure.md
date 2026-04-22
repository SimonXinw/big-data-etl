# 项目结构说明

## 设计原则

这次改版后的目标很明确：

- **只保留 Python**，避免双语言实现增加学习成本
- **目录清晰**，每个文件职责单一
- **先本地、后扩展**，先跑 DuckDB，再走 PostgreSQL / Supabase
- **适合教学**，所以注释和命名尽量直白

## 为什么现在只保留 Python

之前仓库里同时存在 Python 和 Node.js 两套实现，虽然功能更丰富，
但对学习 ETL 来说反而容易分散注意力。

现在统一成 Python 后，学习路径会更顺：

1. 先理解 ETL 主链路
2. 再理解数仓分层
3. 再理解如何发布到 PostgreSQL / Supabase
4. 最后接 BI

## 顶层目录职责

### `lib/`

与 ETL 编排解耦的 **可复用 Python 库**。当前仅包含：

- `lib/shopify/`：通用只读访问——**Admin** 在 `admin.py`（`ShopifyAdminClient.execute`），**Storefront** 在 `storefront.py`；GraphQL 字符串在 `queries/*.py`（按日订单分页在 `etl_project.integrations.shopify.admin.wide_sync` 等业务模块内）

原则：**不写 mutation、不在库内做业务 reshape**（业务侧在 `etl_project/integrations/...`）；采集字段集中在 `queries/*.py`，需要新指标时在对应 query 中追加 selection 即可。

### `etl_project/`

项目主代码目录，负责真正的 ETL 逻辑。

### `data/`

存放学习过程中用到的输入和输出数据。细分约定见 **`data/README.md`**。

- `inbox/`：本地中转（JSON 或 CSV），由 `integrations/inbox/materialize.py` 物化为 `raw/` 下统一列名的 CSV
- `raw/`：进入 DuckDB 的 CSV（可由 inbox、`--source shopify` 衍生表写出，或直接手写）
- `products/`：店面商品等 JSON 导出（如 `storefront_products.json`）
- `orders/`：订单宽表 JSON 快照导出（可选，不落库仅排查时用）
- `output/`：导出结果
- `warehouse/`：本地 DuckDB 文件

### `sql/postgres/`

存放 **标准 PostgreSQL** 的初始化 SQL（`CREATE SCHEMA` / `CREATE TABLE IF NOT EXISTS` 等）。同目录另有 **`supabase_grants_for_datareport.sql`**：对 `anon` / `authenticated` 授予分层表 `SELECT` 与宽松 RLS（供 **datareport** 经 PostgREST 读数；需与 Dashboard「Exposed schemas」一并配置）。

这里刻意不单独建 `supabase/` 目录，原因是：

- **Supabase 的数据库就是 PostgreSQL**：没有另一套「Supabase 专用 DDL 语法」；在 Supabase Dashboard 的 **SQL Editor** 里，把 `sql/postgres/init_warehouse.sql` 整段粘贴执行即可。
- 本仓库把脚本放在 `sql/postgres/`，表示「任意 Postgres 兼容目标都共用这一份」，包括本机 Postgres、RDS、**Supabase Postgres**。

如果你本地使用 **Supabase CLI**（`supabase db push` / migration 工作流），习惯把变更放在 `supabase/migrations/` 下，可以自行在该目录新增 migration，并把当前 `init_warehouse.sql` 的内容迁移过去（或保持一份为源、另一份用工具同步）。**不是放错位置**，只是当前项目采用「单文件 + SQL Editor」的学习路径，没有强制引入 CLI 目录结构。

### `airflow/dags/`

存放 Airflow 调度示例，演示如何调度 Python ETL。

### `bi/`

存放 BI 相关骨架配置，目前提供 Metabase 的本地启动样例。

### `datareport/`

**可选**的本机分层数据预览：静态 HTML（拆分 ES Module + CSS）+ `serve.py` 同源提供只读 `/api/*`。若配置了 **`SUPABASE_URL` + `SUPABASE_PUBLISHABLE_KEY`（或 `SUPABASE_ANON_KEY`）**，默认经 **PostgREST（HTTPS）** 拉数（`/api/supabase-rows`），绕开部分环境下直连 Postgres `5432` 失败；否则回退 **`ETL_POSTGRES_DSN`** 的 `SELECT`。表清单与 `postgres_loader.PUBLISH_PLANS` 对齐（见 `public/assets/table_catalog.json`）；Supabase 权限示例见 **`sql/postgres/supabase_grants_for_datareport.sql`**（与 `init_warehouse.sql` 同目录）。说明见 **`datareport/README.md`**。

### `docs/`

存放项目说明文档，帮助理解结构和演进方向。常用入口：**`local-full-runbook.md`**（按步骤点命令/SQL）、**`local-etl-quickstart.md`**（仅本地 DuckDB）；与命令表并列的还有根目录 **`README.md`** 的 **「命令速查：拉数 → 落库 → 报表」**。

## `etl_project/` 内部模块说明

目录按职责分为两层：

- **`etl/`**：与具体厂商无关的 **通用管线**——编排 `pipeline.py`、`extract` / `load` / `transform`、`quality`、`postgres_loader`、`sync_logging`。
- **`integrations/`**：**业务接入**——按来源分子包（如 `inbox/`、`shopify/`）。Shopify 再拆 **`admin/`**（订单宽表、映射、查询拼装）与 **`storefront/`**（商品导出、探针）；根下保留 **`compat.py`**（测试用别名）、**`api_smoke.py`**（冒烟）。

根目录保留 **`cli.py`、`config.py`、`models.py`、`sync_chains.py`** 等入口与横切配置。

### `etl_project/__main__.py`

让项目可以直接通过：

```bash
python -m etl_project
```

启动。

### `etl_project/cli.py`

负责命令行参数解析。

这里把“如何运行项目”与“如何执行 ETL”分离开，
避免参数解析代码和业务逻辑混在一起。

### `etl_project/config.py`

统一管理：

- 项目路径
- 输出路径
- SQL 文件位置
- PostgreSQL / Supabase 连接串读取方式

这样做的好处是：

- 路径不会散落在各处
- 后续换目录结构时更容易维护

### `etl_project/models.py`

放一些简单的数据结构，比如：

- `SourceFile`
- `PipelineOptions`
- `PipelineResult`

这些结构不是业务核心，但多个模块都会用到，
集中放在一起更清晰。

### `etl_project/integrations/inbox/materialize.py`

**本地中转适配器**：读取 `data/inbox` 下的 `customers` / `orders`（`.json` 优先于 `.csv`），规范字段后写入 `data/raw/*.csv`。

### `etl_project/integrations/shopify/admin/`（Admin 主链路）

- **`wide_sync.py`**：按 UTC 自然日拆分 `updated_at`、逐日分页拉订单宽表 → DuckDB / 可选 Postgres / 可选 JSON 快照；衍生 narrow `raw_*`。
- **`order_mapping.py`**：GraphQL Order → 宽表列。
- **`orders_bi.py`**：宽表 GraphQL 文本拼装。

### `etl_project/integrations/shopify/storefront/`

店面商品：`export_storefront_products_json.py`、`products_probe.py`。

### `etl_project/integrations/shopify/compat.py`

兼容测试用的配置别名与标签映射。

### `etl_project/integrations/shopify/api_smoke.py`

Admin + Storefront 冒烟 CLI。

### `etl_project/etl/`（通用管线）

见包内 **`pipeline.py`（主编排）**、`extract` / `load` / `transform`、`quality`、`postgres_loader`、`sync_logging`。边界：**raw → mart** 的 SQL 与质检在此；**Shopify / inbox** 只在落地 raw 形态前介入。

### `etl_project/sync_chains.py`

把常见运行方式封装成函数（例如 `run_inbox_to_duckdb`、`run_inbox_to_supabase`），方便脚本或调度器 **import 后一行调用**，而不必每次手写 `PipelineOptions` 组合。

### `etl_project/etl/load.py` / `transform.py` / `postgres_loader.py` / `quality.py`

- **load**：目录、DuckDB 连接、CSV→raw、增量追加订单、导出 mart 汇总。
- **transform**：raw→stg→dw→mart 全部 SQL（`mart_sales_summary`、`mart_daily_sales` 等）。
- **postgres_loader**：DuckDB 结果发布到 PostgreSQL / Supabase。
- **quality**：核心质量规则（空表、金额、外键、mart 非空等）。

### `etl_project/etl/pipeline.py`

主编排：准备目录 →（shopify / inbox / csv 分支）→ `load_raw_tables`（若适用）→ `transform` → `quality` → 导出 → 可选 Postgres。

## 数仓分层说明

### raw

保留原始数据，不做复杂加工。

### stg

做清洗、去重、标准化，是最常见的数据准备层。

### dw

构建维表和事实表，形成更清晰的分析模型。

### mart

给 BI 和业务分析直接使用。

## BI 方案说明

当前默认推荐 **Metabase**。

原因：

- 开源
- 容易本地运行
- 对 PostgreSQL / Supabase 友好
- 学习成本低

对应说明见：

- `docs/bi-guide.md`
- `docs/metabase-dashboard-template.md`
- `bi/metabase/docker-compose.yml`

## 这个版本的改进点

这次改版后，仓库重点提升了这些地方：

- 删除了 Node.js 代码，减少认知负担
- 把命令行参数解析独立到 `cli.py`
- 把共享数据结构独立到 `models.py`
- 把 PostgreSQL / Supabase 发布逻辑独立到 `postgres_loader.py`
- 增加了 `quality.py` 用来做质量校验
- 增加了简单可学的增量 ETL 机制
- 增加了 BI 文档和 Metabase 骨架

整体上，这个版本更适合拿来直接学习和继续扩展。
