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

- `lib/shopify/`：Shopify **Admin GraphQL 只读**（`ShopifyAdminClient.execute`、Connection 分页、按文件拆分的 `queries/`）

原则：**不写 mutation、不做业务 reshape**；采集字段集中在 `queries/*.py`，需要新指标时在对应 query 中追加 selection 即可。

### `etl_project/`

项目主代码目录，负责真正的 ETL 逻辑。

### `data/`

存放学习过程中用到的输入和输出数据。

- `inbox/`：本地中转（JSON 或 CSV），由 `data_inbox_sync.py` 物化为 `raw/` 下统一列名的 CSV
- `raw/`：进入 DuckDB 的 CSV（可由 inbox、Shopify 同步或直接手写）
- `output/`：导出结果
- `warehouse/`：本地 DuckDB 文件

### `sql/postgres/`

存放 **标准 PostgreSQL** 的初始化 SQL（`CREATE SCHEMA` / `CREATE TABLE IF NOT EXISTS` 等）。

这里刻意不单独建 `supabase/` 目录，原因是：

- **Supabase 的数据库就是 PostgreSQL**：没有另一套「Supabase 专用 DDL 语法」；在 Supabase Dashboard 的 **SQL Editor** 里，把 `sql/postgres/init_warehouse.sql` 整段粘贴执行即可。
- 本仓库把脚本放在 `sql/postgres/`，表示「任意 Postgres 兼容目标都共用这一份」，包括本机 Postgres、RDS、**Supabase Postgres**。

如果你本地使用 **Supabase CLI**（`supabase db push` / migration 工作流），习惯把变更放在 `supabase/migrations/` 下，可以自行在该目录新增 migration，并把当前 `init_warehouse.sql` 的内容迁移过去（或保持一份为源、另一份用工具同步）。**不是放错位置**，只是当前项目采用「单文件 + SQL Editor」的学习路径，没有强制引入 CLI 目录结构。

### `airflow/dags/`

存放 Airflow 调度示例，演示如何调度 Python ETL。

### `bi/`

存放 BI 相关骨架配置，目前提供 Metabase 的本地启动样例。

### `docs/`

存放项目说明文档，帮助理解结构和演进方向。

## `etl_project/` 内部模块说明

当前 `etl_project/` 采用**平铺**（一个包内多个 `.py`），对现在这个体量的学习型项目来说是合理默认：

- 文件数量不多，跳转成本低，适合按 README 里的「学习顺序」从上到下读。
- 模块边界已经比较清晰：`pipeline` 编排、`load`/`transform`/`postgres_loader` 分层、`shopify_sync` 采集、`quality` 质检等。

什么时候值得拆子包（例如 `etl_project/sources/`、`etl_project/io/`）？常见触发条件是：采集端种类变多、测试需要大量 mock、或多人并行改同一包冲突明显。那时再拆，比过早分包更省事。

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

### `etl_project/extract.py`

负责抽取阶段的准备工作：

- 列出输入文件
- 检查文件是否存在

这里故意保持简单，因为学习型项目的重点不在复杂采集，而在 ETL 主链路。

### `etl_project/data_inbox_sync.py`

**本地中转适配器**：读取 `data/inbox` 下的 `customers` / `orders`（`.json` 优先于 `.csv`），规范字段后写入 `data/raw/*.csv`，再走与 CSV 源相同的 DuckDB 流程。

### `etl_project/shopify_sync.py`

编排层里的 **Shopify 适配器**：调用 `lib.shopify` 分页拉取客户与订单，把 GraphQL `node` 映射为 `data/raw` 下标准 CSV 列。

边界划分：

- **获取**：`lib.shopify`（通用 HTTP + GraphQL + 分页）
- **源系统 -> raw 列**：`shopify_sync.py`
- **raw -> mart**：`load.py` / `transform.py`（与数据源无关）

### `etl_project/sync_chains.py`

把常见运行方式封装成函数（例如 `run_inbox_to_duckdb`、`run_inbox_to_supabase`），方便脚本或调度器 **import 后一行调用**，而不必每次手写 `PipelineOptions` 组合。

### `etl_project/load.py`

负责本地 DuckDB 的加载阶段：

- 创建目录
- 打开 DuckDB 连接
- 读取 CSV 到 raw 层
- 在增量模式下按 `order_id` 追加新订单
- 导出 mart 汇总 CSV

### `etl_project/transform.py`

负责所有 DuckDB SQL 转换逻辑。

分层顺序：

1. `raw`
2. `stg`
3. `dw`
4. `mart`

这样学习者可以更直观地理解数仓分层。

当前 `mart` 层已经不只是一个简单汇总表，而是拆成了几类更接近业务分析的指标表：

- `mart_sales_summary`
- `mart_daily_sales`
- `mart_city_sales`
- `mart_customer_level_sales`

这样更方便后续接 BI。

### `etl_project/postgres_loader.py`

负责把 DuckDB 已经转换好的结果，发布到 PostgreSQL / Supabase。

这样做的好处是：

- 只维护一套转换逻辑
- 本地学习和远程落库共用一套 ETL 主线
- 不会因为数据库不同而重复开发两套代码

### `etl_project/quality.py`

负责数据质量校验。

这个模块的目标不是做特别重的平台，而是把最关键的质量规则先固化下来，
比如：

- 原始表不能为空
- 订单金额不能非法
- 事实表外键关系不能断裂
- mart 层必须有结果

这样更贴近真实项目里的 ETL 思路。

### `etl_project/pipeline.py`

这是整个项目最核心的编排层。

它负责把下面这些步骤串起来：

1. 准备目录
2. 如选择 Shopify 数据源，则先同步到 `data/raw` 的 CSV
3. 如选择 inbox 数据源，则先把 `data/inbox` 物化到 `data/raw` 的 CSV
4. 检查输入文件
5. 加载 raw 层
6. 执行 SQL 转换
7. 执行数据质量校验
8. 如有需要，按增量规则追加订单
9. 导出 CSV
10. 按需发布到 PostgreSQL / Supabase

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
