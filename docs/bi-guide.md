# BI 层接入说明

## 默认推荐：Metabase

这个项目默认推荐 **Metabase** 作为 BI 工具，原因很简单：

- 免费、开源
- 本地启动相对简单
- 对 PostgreSQL / Supabase 很友好
- 对学习型项目非常合适，能很快看到报表效果

## 为什么不是直接在 DuckDB 上做 BI

DuckDB 很适合本地 ETL 和学习 SQL，但真正做 BI 时，
更稳定、更通用的做法仍然是：

1. Python ETL 在本地用 DuckDB 做抽取和转换
2. 如果需要 BI，就把结果发布到 PostgreSQL / Supabase
3. Metabase 连接 PostgreSQL / Supabase 生成仪表盘

这样更接近真实业务里的做法。

## 推荐的数据链路

```text
CSV / 埋点数据
    -> Python ETL
    -> DuckDB（本地转换）
    -> PostgreSQL / Supabase（分析仓库）
    -> Metabase（BI）
```

## 推荐优先看的表

在 Metabase 里，建议先从这些表建图表：

- `mart_daily_sales`（DuckDB 本地文件名/表名）
- `mart_city_sales`
- `mart_customer_level_sales`
- `mart_sales_summary`

如果你把结果发布到了 PostgreSQL / Supabase，对应 schema 一般是 `mart`，表名会是：

- `mart.big_data_etl_daily_sales`
- `mart.big_data_etl_city_sales`
- `mart.big_data_etl_customer_level_sales`
- `mart.big_data_etl_sales_summary`

其中最适合入门做 BI 的是：

- `mart_daily_sales`
- `mart_city_sales`

因为它们已经是聚合后的结果，适合快速做：

- 每日 GMV
- 城市销售额
- 客户等级销售贡献
- 平均订单金额

## 建议先做的 4 个仪表盘

1. 月销售额趋势
2. 城市销售额排行
3. 客户等级销售贡献占比
4. 平均订单金额变化

如果你想直接照着搭看板，仓库已经补了模板文档：

- `docs/metabase-dashboard-template.md`

里面已经写清楚：

- 每张图该用哪张表
- 每张图看什么指标
- 每张图回答什么业务问题
- 仪表盘应该怎么布局

## 本地启动方式

仓库已经提供：

- `bi/metabase/docker-compose.yml`

启动后，Metabase 默认端口为 `3000`。

## 接入步骤

1. 先运行 Python ETL
2. 如果要给 BI 使用，再执行 PostgreSQL / Supabase 目标落库
3. 启动 Metabase
4. 在 Metabase 中连接 PostgreSQL / Supabase
5. 参考 `docs/metabase-dashboard-template.md` 开始建仪表盘

## 对这个学习项目的建议

如果你只是先学 ETL：

- 先用 DuckDB 本地跑通

如果你要继续往“数据产品 / 数据分析 / BI”方向走：

- 再把结果发到 PostgreSQL / Supabase
- 用 Metabase 接上分析层
