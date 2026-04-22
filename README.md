# big-data-etl

一个**纯 Python** 的学习型 ETL 项目。

这个仓库现在只保留一套 Python 实现，目标是：

- 本地容易跑起来
- 目录清晰、职责明确
- 注释友好，适合学习
- 既能先学本地 ETL，也能继续走到 PostgreSQL / Supabase + BI

## 适合什么场景

这个项目适合：

- 想学习 ETL 基本流程的人
- 想理解 `extract -> load raw -> transform -> mart` 的人
- 想从本地 DuckDB 逐步过渡到 PostgreSQL / Supabase + BI 的人

## 项目特点

- **纯 Python**：不再混用 Node.js
- **本地默认 DuckDB**：最容易起步
- **可选 PostgreSQL / Supabase 发布**：更接近真实业务
- **数仓分层清晰**：`raw -> stg -> dw -> mart`
- **内置数据质量校验**：跑完 ETL 后自动检查关键规则
- **支持简单增量 ETL**：按 `order_id` 追加新订单
- **可选 Shopify Admin 订单拉数**：按日 `updated_at` 宽表进 DuckDB / 可选 Supabase，与 **data/orders/** JSON 快照导出
- **可选本地 inbox 中转**：`data/inbox` → 物化为 `data/raw` 再进 DuckDB
- **BI 推荐明确**：默认推荐 Metabase

## 目录结构

`data/` 下各子目录用途见 **`data/README.md`**（产品与订单 JSON、raw、导出物等约定）。

```text
big-data-etl/
├─ airflow/
│  └─ dags/
│     └─ python_etl_dag.py         # Airflow 调度示例
├─ bi/
│  └─ dataease/
│     ├─ README.md                 # DataEase：默认账号、MariaDB 说明、启停命令
│     └─ docker-compose.yml        # DataEase + 内置 MariaDB（BI 前端）
├─ datareport/
│  ├─ README.md                    # 分层报表：Supabase REST / Postgres、启动与前置 SQL
│  ├─ serve.py                     # 静态页 + 只读 /api（读 SUPABASE_* 与/或 ETL_POSTGRES_DSN）
│  └─ public/                      # index.html、拆分 css/js、table_catalog.json
├─ data/
│  ├─ README.md                    # data 子目录约定说明
│  ├─ inbox/                       # 本地中转：json/csv → 物化为 raw
│  ├─ raw/                         # 进入 DuckDB 的 CSV
│  ├─ products/                    # 店面商品等 JSON 导出（如 storefront_products.json）
│  ├─ orders/                      # 订单宽表 JSON 快照（可选导出）
│  ├─ output/                      # 导出的分析结果
│  └─ warehouse/                   # DuckDB 数据文件
├─ docs/
│  ├─ bi-guide.md                  # BI 选型与接入说明
│  ├─ local-full-runbook.md        # 本地全流程：SQL、ETL、Postgres、datareport、可选 BI
│  ├─ local-etl-quickstart.md      # 仅本地 DuckDB ETL 快速上手
│  └─ project-structure.md         # 模块职责说明
├─ lib/
│  └─ shopify/                     # Shopify Admin GraphQL 只读客户端与 query 定义
├─ etl_project/
│  ├─ __init__.py
│  ├─ __main__.py                  # python -m etl_project 入口
│  ├─ cli.py                       # 命令行参数解析
│  ├─ config.py                    # 路径和环境配置
│  ├─ models.py                    # 轻量数据模型
│  ├─ etl/                         # **通用管线**（与具体厂商解耦）
│  │   ├─ pipeline.py              # 主编排
│  │   ├─ extract.py / load.py / transform.py
│  │   ├─ quality.py / postgres_loader.py / sync_logging.py
│  ├─ integrations/                # **业务接入**（厂商 / 本地格式）
│  │   ├─ inbox/materialize.py    # data/inbox → data/raw CSV
│  │   └─ shopify/
│  │       ├─ admin/               # Admin：wide_sync、order_mapping、orders_bi
│  │       ├─ storefront/          # Storefront：商品导出、探针
│  │       ├─ compat.py           # 兼容测试用配置别名与标签映射
│  │       └─ api_smoke.py        # Admin + Storefront 冒烟
│  └─ sync_chains.py               # 常用链路封装（inbox→DuckDB / inbox→Supabase 等）
├─ sql/
│  └─ postgres/
│     ├─ init_warehouse.sql                    # PostgreSQL / Supabase 初始化 DDL
│     └─ supabase_grants_for_datareport.sql  # Supabase PostgREST：anon 读分层表（datareport）
├─ tests/
│  ├─ test_pipeline.py             # Python 自测
│  ├─ test_shopify_sync.py         # Shopify 同步相关自测
│  ├─ test_shopify_lib.py          # lib.shopify 通用逻辑自测
│  └─ test_inbox_sync.py           # inbox 物化与 ETL 自测
├─ .env.example
├─ .gitignore
├─ pyrightconfig.json
└─ requirements.txt
```

## ETL 主流程

这个项目模拟一个简单电商业务：

1. 从 CSV 读取客户和订单数据（或从 `data/inbox` 物化成 raw CSV，或 **`--source shopify`** 由宽表衍生 narrow raw）
2. 先加载到 DuckDB 的 raw 层
3. 在 DuckDB 中完成清洗和建模
4. 产出 stg / dw / mart 层
5. 自动执行数据质量校验
6. 如果开启增量模式，就只把新订单追加到 raw 层
7. 导出一份汇总 CSV
8. 如果需要，再把结果发布到 PostgreSQL / Supabase
9. BI 工具从 PostgreSQL / Supabase 读取汇总数据做仪表盘

## 本地 inbox 中转 vs Supabase（两条链路）

| 链路 | 含义 | 典型命令 / 代码入口 |
|------|------|---------------------|
| **本地中转** | 只使用仓库内 `data/inbox/*.json`（或 `.csv`）→ 覆盖写入 `data/raw/*.csv` → DuckDB 分层 → `data/output` | `python -m etl_project --source inbox --target duckdb` 或 `from etl_project.sync_chains import run_inbox_to_duckdb` |
| **Supabase / PostgreSQL** | 与上相同的转换引擎（DuckDB），最后把 mart 等表 **发布** 到远程 Postgres（含 Supabase 托管库） | `python -m etl_project --source inbox --target postgres`（需 `.env` 中 `ETL_POSTGRES_DSN`）或 `from etl_project.sync_chains import run_inbox_to_supabase` |

**inbox 文件约定**（`data/inbox/`，JSON 优先于同名 CSV）：

- `customers.json` 或 `customers.csv`
- `orders.json` 或 `orders.csv`

JSON 推荐顶层为**数组**；也支持根对象内含 `customers` / `orders` / `data` 数组字段。列经 `integrations/inbox/materialize.py` 规范为与 `data/raw` 示例相同的字段名后，再走统一 `etl/transform.py`。

仓库内已带一份与示例数据等价的 **`data/inbox/customers.json`**、**`orders.json`**，可直接跑 `--source inbox` 做联调。

## 数据分层说明

### 1. raw

保留最接近原始输入的数据。

表：

- `raw_customers`
- `raw_orders`

### 2. stg

做基础清洗、去重、标准化。

表：

- `stg_customers`
- `stg_orders`

### 3. dw

面向分析建维表和事实表。

表：

- `dim_customers`
- `fact_orders`

### 4. mart

给 BI 和业务报表直接使用。

表：

- `mart_sales_summary`
- `mart_daily_sales`
- `mart_city_sales`
- `mart_customer_level_sales`

这些表比之前更接近真实业务分析会看的指标，比如：

- 日 GMV
- 日订单数
- 客单价
- 城市销售贡献
- 客户等级销售贡献

## 数据质量校验

项目现在会在每次 ETL 运行后自动执行一组基础质量检查。

当前默认检查包括：

- `raw_customers` 不能为空
- `raw_orders` 不能为空
- `stg_orders` 中订单金额必须大于 0
- `dim_customers` 中 `customer_id` 必须唯一
- `fact_orders.customer_id` 必须能关联到 `dim_customers`
- `mart_sales_summary` 至少要有一行结果
- `mart_daily_sales` 至少要有一行结果

运行后会额外生成：

- `data/output/quality_report.json`

这个文件适合：

- 自查 ETL 是否可信
- 给测试或调度层做结果判断
- 帮助学习者理解“ETL 跑通”和“数据可信”是两回事

## Shopify 后台数据接入（订单 -> 同一套 ETL -> BI）

Shopify 侧使用 **`lib/shopify` 封装的 Admin GraphQL**（自定义应用 Access Token）。**主链路**为 `etl_project/integrations/shopify/admin/wide_sync.py`：按 **`--shopify-days` / `ETL_SHOPIFY_SYNC_DAYS`** 回溯若干 **UTC 自然日**，**每日单独**查询该日内 `updated_at` 窗口 → 订单宽表写入 DuckDB（可选 **PostgreSQL `raw.big_data_etl_shopify_orders`** UPSERT、**`data/orders/`** JSON 快照）。随后由衍生的 `raw_orders` / `raw_customers` 进入 **`transform.py`**。

**端到端说明**：`docs/shopify-and-bi-pipeline.md`。

### 1. 准备环境变量

参考 `.env.example`，至少需要：

```env
SHOPIFY_STORE_DOMAIN=your-store.myshopify.com
SHOPIFY_ADMIN_ACCESS_TOKEN=shpat_xxx
```

可选：默认 Admin API 版本 **`2025-04`**（可用 `SHOPIFY_ADMIN_API_VERSION` / `SHOPIFY_API_VERSION` 覆盖）、回溯天数 **`ETL_SHOPIFY_SYNC_DAYS`（默认 14）**、订单 JSON 开关 **`ETL_SHOPIFY_JSON_SNAPSHOT_ENABLE`**（默认开启，写入固定路径 `data/orders/shopify_orders_snapshot.json`）、**`ETL_POSTGRES_DSN`**。

### 2. 运行 ETL（Shopify 数据源）

```bash
python -m etl_project --source shopify --target duckdb
```

可选：仅验证 Admin / Storefront 通路（不跑 ETL）：

```bash
python -m etl_project --smoke-shopify-apis
```

若要把结果推到 PostgreSQL / Supabase 给 Metabase 使用：

```bash
python -m etl_project --source shopify --target postgres
```

说明：

- 衍生 narrow 层里 `customer_level` 当前为占位 **`standard`**（与宽表学习路径一致）；若需按 tags 映射可扩展 `integrations/shopify/admin/order_mapping` / 衍生 SQL
- 游客订单使用与简版同步一致的合成 `customer_legacy_id` 规则
- 新增业务字段：GraphQL 见 **`lib/shopify/queries/orders.py`**（宽表模板 `ORDERS_BY_UPDATED_DAY_TEMPLATE`），单日组装逻辑见 **`etl_project/integrations/shopify/admin/orders_bi.py`**；映射改 **`integrations/shopify/admin/order_mapping.py`**；必要时同步 **`sql/postgres/init_warehouse.sql`** 中 `raw.big_data_etl_shopify_orders`

### 3. 看 BI 报表

流程与 CSV 场景一致：PostgreSQL / Supabase 落库后，用 Metabase 连接数据库，优先使用 `mart` schema 下的 `big_data_etl_daily_sales` / `big_data_etl_city_sales` / `big_data_etl_customer_level_sales` / `big_data_etl_sales_summary` 建图（详见 `docs/bi-guide.md` 与 `docs/metabase-dashboard-template.md`）。

## 快速开始

### 1. 安装依赖

```bash
python -m pip install -r requirements.txt
```

### 2. 跑本地 DuckDB 版

```bash
python -m etl_project
```

或者显式指定目标：

```bash
python -m etl_project --target duckdb
```

使用 **inbox 本地中转**（先物化 `data/inbox` → `data/raw`，再跑 ETL）：

```bash
python -m etl_project --source inbox --target duckdb
```

### 3. 跑测试

```bash
python -m unittest discover -s tests -v
```

## 命令速查：拉数 → 落库 → 报表

统一入口均为项目根目录下的 **`python -m etl_project`**（或 `scripts/dev` 里同名场景的 `*_dev.ps1`）。**拉数**由 `--source` 决定原料；**落库**由 `--target` 决定只写本地 DuckDB 还是再发布到 Postgres。

| 场景 | 命令 | 数据落在哪 |
|------|------|------------|
| **本地拉数，只存本地（DuckDB）** | `python -m etl_project --source csv --target duckdb` | `data/warehouse/etl_learning.duckdb`，导出 `data/output/*` |
| **inbox 中转 → 本地** | `python -m etl_project --source inbox --target duckdb` | 同上；原料来自 `data/inbox` 物化到 `data/raw` |
| **Shopify → 本地** | `python -m etl_project --source shopify --target duckdb` | 同上；需 `.env` 中 Shopify Admin 变量 |
| **本地拉数，存 Postgres / Supabase** | `python -m etl_project --source csv --target postgres` | 远端库表 `raw/stg/dw/mart` 下 `big_data_etl_*`；需已执行 **`sql/postgres/init_warehouse.sql`** 且配置 **`ETL_POSTGRES_DSN`** |
| **inbox → Postgres** | `python -m etl_project --source inbox --target postgres` | 同上 |
| **Shopify → Postgres** | `python -m etl_project --source shopify --target postgres` | 同上 |
| **打开本仓库分层报表（HTML 服务）** | `python datareport/serve.py` | 浏览器访问 **`http://127.0.0.1:8787/`**（默认端口）；Windows 可用 **`.\scripts\dev\serve_datareport.ps1`** |

**报表读数说明**：`datareport` 若配置了 **`SUPABASE_URL` + `SUPABASE_PUBLISHABLE_KEY`（或 `SUPABASE_ANON_KEY`）**，默认经 **PostgREST（HTTPS）** 拉数；否则回退 **`ETL_POSTGRES_DSN`** 直连。Supabase 需在 Dashboard 暴露 schema，并可选执行 **`sql/postgres/supabase_grants_for_datareport.sql`**。详见 **`datareport/README.md`**。

更细的「点哪些文件 / 命令」清单见 **`docs/local-full-runbook.md`**；Windows 一键脚本说明见 **`scripts/dev/README.md`**。

## 增量 ETL 怎么用

现在项目支持一个**简单可学的增量模式**。

规则很直接：

- 订单数据以 `order_id` 作为增量唯一键
- 已经写入 `raw_orders` 的订单不会重复插入
- 新订单会被追加进 `raw_orders`
- 后续 `stg / dw / mart` 仍然基于累计后的 raw 数据重新构建

这个方案的优点是：

- 很容易理解
- 比较接近真实业务里的“订单追加”场景
- 又不会把学习项目做得太复杂

运行方式：

```bash
python -m etl_project --target duckdb --incremental
```

命令输出里会显示：

- 当前是否是增量模式
- 本次新增了多少订单行

## PostgreSQL / Supabase 版怎么用

### 1. 初始化数据库

先在 PostgreSQL 或 Supabase SQL Editor 执行：

```sql
sql/postgres/init_warehouse.sql
```

说明：`init_warehouse.sql` 写的是**标准 PostgreSQL** DDL，并不是另一种「Supabase 格式」。Supabase 提供的是托管 Postgres，因此脚本放在 `sql/postgres/` 是刻意设计（与自建 Postgres 共用），**不是路径放错**。若你使用 Supabase CLI 的 `supabase/migrations/` 工作流，可自行把同等 DDL 迁到该目录；用 Dashboard SQL Editor 则直接执行本文件即可。

### 2. 配置环境变量

将 `.env.example` 复制为**项目根目录**下的 **`.env`**（与 `requirements.txt` 同级），再编辑其中的值。运行 `python -m etl_project` 时会自动加载该文件（通过 `python-dotenv`）。

参考：`.env.example`

核心变量：

```env
ETL_POSTGRES_DSN=postgresql://postgres:password@localhost:5432/etl_learning
```

如果你用的是 Supabase，也可以直接使用 Supabase 的 PostgreSQL 连接串。

### 3. 把结果发布到 PostgreSQL / Supabase

```bash
python -m etl_project --target postgres
```

从 **inbox** 物化 raw 后直接发布到同一套远程库：

```bash
python -m etl_project --source inbox --target postgres
```

或在代码里：`from etl_project.sync_chains import run_inbox_to_supabase`（等价于上式，便于脚本复用）。

### 4. 本机浏览分层表（datareport，可选）

发布到 Supabase / Postgres 后，可用仓库内 **`datareport/`** 启动**只读** Web 服务（KPI 块 + 明细表 + 可选图表）。**不修改 ETL 逻辑**；与上文「命令速查」中报表一行一致，详见 **`datareport/README.md`**。

```bash
python datareport/serve.py
```

Windows 也可：`.\scripts\dev\serve_datareport.ps1`

## 运行后会得到什么

默认本地运行后会生成：

- `data/warehouse/etl_learning.duckdb`
- `data/output/sales_summary.csv`
- `data/output/quality_report.json`

如果你启用了增量模式，命令行还会输出：

- `本次新增订单行数`

如果你使用 PostgreSQL / Supabase 目标，还会把这些表发布出去：

- `raw.big_data_etl_customers_raw`
- `raw.big_data_etl_orders_raw`
- `stg.big_data_etl_customers_clean`
- `stg.big_data_etl_orders_clean`
- `dw.big_data_etl_dim_customers`
- `dw.big_data_etl_fact_orders`
- `mart.big_data_etl_sales_summary`
- `mart.big_data_etl_daily_sales`
- `mart.big_data_etl_city_sales`
- `mart.big_data_etl_customer_level_sales`

## 现在已经支持的业务指标

目前项目已经能直接产出这些比较实用的指标：

- **GMV**：`gross_merchandise_value`
- **订单数**：`order_count`
- **客户数**：`customer_count`
- **客单价**：`average_order_value`

对应的 mart 表：

- `mart_daily_sales`：按天看趋势
- `mart_city_sales`：按城市看贡献
- `mart_customer_level_sales`：按客户等级看结构
- `mart_sales_summary`：按城市 + 客户等级 + 月份做综合汇总

## BI 怎么接

默认推荐：**Metabase**

原因：

- 免费、开源
- 本地部署简单
- 对 PostgreSQL / Supabase 友好
- 学习成本低

仓库中已经提供：

- `docs/bi-guide.md`
- `docs/metabase-dashboard-template.md`
- `bi/dataease/docker-compose.yml`（DataEase；**默认登录与内置库说明见 `bi/dataease/README.md`**）

推荐链路：

```text
Python ETL -> PostgreSQL / Supabase -> BI（Metabase / DataEase 等）
```

如果你想直接照着搭仪表盘，可以看：

- `docs/metabase-dashboard-template.md`

这个模板已经给出：

- 推荐图表清单
- 每张图对应的 mart 表
- 指标口径
- 仪表盘布局建议
- 可以回答的业务问题

## 学习顺序建议

建议按这个顺序看代码：

1. `etl_project/etl/pipeline.py`
2. `etl_project/cli.py`
3. `etl_project/config.py`
4. `etl_project/etl/extract.py`
5. `etl_project/integrations/inbox/materialize.py`
6. `etl_project/integrations/shopify/compat.py`
7. `etl_project/sync_chains.py`
8. `etl_project/etl/load.py`
9. `etl_project/etl/transform.py`
10. `etl_project/etl/postgres_loader.py`
11. `etl_project/etl/quality.py`
12. `tests/test_pipeline.py`
13. `tests/test_inbox_sync.py`
14. `tests/test_shopify_sync.py`
15. `docs/bi-guide.md`
16. `docs/metabase-dashboard-template.md`

如果你是做业务分析，建议优先看：

1. `mart_daily_sales`
2. `mart_city_sales`
3. `mart_customer_level_sales`
4. `mart_sales_summary`

## 后续还可以怎么升级

- 把 CSV 数据源换成 API / 埋点 / 消息队列
- 把当前简单增量模式升级成更完整的 watermark / 分区增量同步
- 把 Airflow DAG 升级成多任务 DAG
- 为 mart 层补更多业务指标
- 把当前 Metabase 模板继续扩成可直接导入的正式看板
