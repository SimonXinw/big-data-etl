-- Supabase PostgREST：允许「匿名 / 登录」角色读取 ETL 分层表（供 datareport 等客户端经 REST 拉数）
--
-- 使用方式
-- --------
-- 1) 在 Supabase Dashboard → Settings → API →「Exposed schemas」中加入：raw, stg, dw, mart（与项目一致）。
-- 2) 在本项目的 SQL Editor 中整段执行本脚本（可重复执行：先 DROP POLICY IF EXISTS 再 CREATE）。
--
-- 安全说明
-- --------
-- - 以下策略对 anon / authenticated 放开 SELECT，适合**学习 / 内网演示**。
-- - 生产环境请改为更严格的 RLS（按租户、按角色），或仅使用服务端服务密钥在受控服务中查数。

grant usage on schema raw to anon, authenticated;
grant usage on schema stg to anon, authenticated;
grant usage on schema dw to anon, authenticated;
grant usage on schema mart to anon, authenticated;

grant select on all tables in schema raw to anon, authenticated;
grant select on all tables in schema stg to anon, authenticated;
grant select on all tables in schema dw to anon, authenticated;
grant select on all tables in schema mart to anon, authenticated;

alter table raw.big_data_etl_customers_raw enable row level security;
alter table raw.big_data_etl_orders_raw enable row level security;
alter table stg.big_data_etl_customers_clean enable row level security;
alter table stg.big_data_etl_orders_clean enable row level security;
alter table dw.big_data_etl_dim_customers enable row level security;
alter table dw.big_data_etl_fact_orders enable row level security;
alter table mart.big_data_etl_sales_summary enable row level security;
alter table mart.big_data_etl_daily_sales enable row level security;
alter table mart.big_data_etl_city_sales enable row level security;
alter table mart.big_data_etl_customer_level_sales enable row level security;

drop policy if exists datareport_read_raw_customers on raw.big_data_etl_customers_raw;
create policy datareport_read_raw_customers on raw.big_data_etl_customers_raw
  for select to anon, authenticated using (true);

drop policy if exists datareport_read_raw_orders on raw.big_data_etl_orders_raw;
create policy datareport_read_raw_orders on raw.big_data_etl_orders_raw
  for select to anon, authenticated using (true);

drop policy if exists datareport_read_stg_customers on stg.big_data_etl_customers_clean;
create policy datareport_read_stg_customers on stg.big_data_etl_customers_clean
  for select to anon, authenticated using (true);

drop policy if exists datareport_read_stg_orders on stg.big_data_etl_orders_clean;
create policy datareport_read_stg_orders on stg.big_data_etl_orders_clean
  for select to anon, authenticated using (true);

drop policy if exists datareport_read_dw_dim_customers on dw.big_data_etl_dim_customers;
create policy datareport_read_dw_dim_customers on dw.big_data_etl_dim_customers
  for select to anon, authenticated using (true);

drop policy if exists datareport_read_dw_fact_orders on dw.big_data_etl_fact_orders;
create policy datareport_read_dw_fact_orders on dw.big_data_etl_fact_orders
  for select to anon, authenticated using (true);

drop policy if exists datareport_read_mart_sales_summary on mart.big_data_etl_sales_summary;
create policy datareport_read_mart_sales_summary on mart.big_data_etl_sales_summary
  for select to anon, authenticated using (true);

drop policy if exists datareport_read_mart_daily_sales on mart.big_data_etl_daily_sales;
create policy datareport_read_mart_daily_sales on mart.big_data_etl_daily_sales
  for select to anon, authenticated using (true);

drop policy if exists datareport_read_mart_city_sales on mart.big_data_etl_city_sales;
create policy datareport_read_mart_city_sales on mart.big_data_etl_city_sales
  for select to anon, authenticated using (true);

drop policy if exists datareport_read_mart_customer_level_sales on mart.big_data_etl_customer_level_sales;
create policy datareport_read_mart_customer_level_sales on mart.big_data_etl_customer_level_sales
  for select to anon, authenticated using (true);
