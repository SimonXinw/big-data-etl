# Metabase 仪表盘模板

这份文档提供一个可以直接照着搭的 Metabase 仪表盘模板。

目标不是做复杂 BI 平台，而是让你把当前 ETL 项目的结果，
快速变成一套能讲业务的看板。

## 仪表盘名称建议

- `跨境电商经营总览`

## 建议使用的数据表

优先使用这 4 张 mart 表：

1. `mart_daily_sales`
2. `mart_city_sales`
3. `mart_customer_level_sales`
4. `mart_sales_summary`

如果你连的是 PostgreSQL / Supabase（推荐用于 Metabase），字段口径保持一致，但表一般在 `mart` schema 下，名称对应为 `big_data_etl_daily_sales` / `big_data_etl_city_sales` / `big_data_etl_customer_level_sales` / `big_data_etl_sales_summary`。

## 核心指标口径

### GMV

- 字段：`gross_merchandise_value`
- 含义：一段时间内的订单总销售额

### 订单数

- 字段：`order_count`
- 含义：一段时间内的订单数量

### 客户数

- 字段：`customer_count`
- 含义：一段时间内下单客户数量

### 客单价

- 字段：`average_order_value`
- 含义：平均每单金额

## 推荐看板结构

建议把一个仪表盘拆成 3 层：

1. **总览指标区**
2. **趋势分析区**
3. **结构分析区**

## 图表 1：核心经营指标卡片

### 图表类型

- Number / KPI 卡片

### 来源表

- `mart_daily_sales`

### 推荐展示的 4 个卡片

1. 总 GMV
2. 总订单数
3. 总客户数
4. 平均客单价

### 建议聚合方式

- GMV：`SUM(gross_merchandise_value)`
- 订单数：`SUM(order_count)`
- 客户数：`SUM(customer_count)`
- 客单价：`AVG(average_order_value)`

## 图表 2：每日 GMV 趋势

### 图表类型

- Line Chart

### 来源表

- `mart_daily_sales`

### 横轴

- `order_date`

### 指标

- `gross_merchandise_value`

### 这个图回答什么问题

- 每天销售额有没有上涨或回落？
- 有没有明显波峰波谷？

## 图表 3：每日订单数趋势

### 图表类型

- Bar Chart 或 Line Chart

### 来源表

- `mart_daily_sales`

### 横轴

- `order_date`

### 指标

- `order_count`

## 图表 4：城市销售贡献排行

### 图表类型

- Horizontal Bar Chart

### 来源表

- `mart_city_sales`

### 维度

- `city`

### 指标

- `gross_merchandise_value`

## 图表 5：客户等级销售结构

### 图表类型

- Pie Chart / Stacked Bar Chart

### 来源表

- `mart_customer_level_sales`

### 维度

- `customer_level`

### 指标

- `gross_merchandise_value`

## 图表 6：城市 + 客户等级交叉分析

### 图表类型

- Table / Pivot Table

### 来源表

- `mart_sales_summary`

### 行维度

- `city`

### 列维度

- `customer_level`

### 指标

- `total_sales`
- `order_count`

## 建议加的筛选器

建议在 Metabase 里统一加这些筛选器：

1. `order_month`
2. `city`
3. `customer_level`

## 最小可用布局建议

### 第一行：经营总览

- 总 GMV
- 总订单数
- 总客户数
- 平均客单价

### 第二行：趋势

- 每日 GMV 趋势
- 每日订单数趋势

### 第三行：结构

- 城市销售贡献排行
- 客户等级销售结构

### 第四行：交叉分析

- 城市 × 客户等级透视表

## 建议的分析顺序

如果你第一次看这个看板，建议这样分析：

1. 先看 GMV / 订单数 / 客单价
2. 再看每日趋势
3. 再看城市贡献
4. 再看客户等级结构
5. 最后看城市与客户等级的交叉关系

## 适合继续升级的方向

这套模板后面还可以继续加：

- 新老客拆分
- 国家 / 渠道维度
- 商品品类维度
- 转化漏斗
- 复购分析
