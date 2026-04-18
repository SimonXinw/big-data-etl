-- PostgreSQL / Supabase：数仓分层初始化（raw -> stg -> dw -> mart）
--
-- 【与已有业务库共存】
-- - 本脚本**只**创建 schema：raw、stg、dw、mart，以及其中的表与索引。
-- - **不会**对 `public` 或其他 schema 执行 DROP / TRUNCATE；也不会修改你已有的业务表。
-- - 若贵司已在这些 schema 名下建了同名对象，请先确认命名无冲突；需要换名请自行调整本文件中的 schema 名。
--
-- 【首次部署】
-- - 空库或尚未存在上述表：在 Supabase SQL Editor 中整段执行即可（幂等：`IF NOT EXISTS`）。
--
-- 【旧版已跑过早期 init、且表结构少于当前 Python ETL】
-- - `CREATE TABLE IF NOT EXISTS` **不会**给旧表自动加新列。
-- - 请另执行（可选）`migrate_legacy_etl_additive.sql` 做增量加列；仍不删除业务数据。
-- - 若 mart 表主键与当前脚本不一致（例如旧 daily_sales 仅按 order_date），需单独评估迁移，勿盲跑 DROP。

create schema if not exists raw;
create schema if not exists stg;
create schema if not exists dw;
create schema if not exists mart;

create table if not exists raw.customers_raw (
    customer_id bigint primary key,
    customer_name text not null,
    city text not null,
    customer_level text not null,
    source_system text not null default 'csv_demo',
    etl_loaded_at timestamptz not null default now()
);

create table if not exists raw.orders_raw (
    order_id bigint primary key,
    customer_id bigint not null,
    order_amount numeric(12, 2) not null,
    order_date date not null,
    currency_code text not null default 'CNY',
    ship_country text not null default 'UNKNOWN',
    refund_amount numeric(12, 2) not null default 0,
    source_system text not null default 'csv_demo',
    etl_loaded_at timestamptz not null default now()
);

-- Shopify Admin 宽表（与 bi-database `shopify_orders` 字段对齐；按 shopify_gid 增量 UPSERT）
create table if not exists raw.shopify_orders (
    shopify_gid text primary key,
    shop_name text not null,
    order_id text,
    legacy_order_id bigint,
    customer_legacy_id bigint,
    created_at text,
    beijing_created_at text,
    use_created_at text,
    berlin_created_at text,
    order_status text,
    product_quantity integer,
    total_product_amount double precision,
    shippingfee double precision,
    total_product_discount double precision,
    discountfee double precision,
    tax double precision,
    logistics_status text,
    marketing_intention integer,
    currency_code text,
    total_price double precision,
    product_sales double precision,
    totalrefunded double precision,
    discount_method text,
    customer_display_name text,
    customer_email text,
    customer_phone text,
    bill_country text,
    shipping_address1 text,
    shipping_address2 text,
    shipping_city text,
    shipping_province text,
    shipping_country text,
    shipping_zip text,
    shipping_phone text,
    shipping_name text,
    discount_code text,
    updated_at text,
    cancelled_at text,
    cancel_reason text,
    product_details text,
    discount_information text,
    billing_address text,
    refunds text,
    etl_synced_at timestamptz not null default now()
);

create index if not exists idx_shopify_orders_legacy_id on raw.shopify_orders (legacy_order_id);

create table if not exists stg.customers_clean (
    customer_id bigint primary key,
    customer_name text not null,
    city text not null,
    customer_level text not null,
    etl_processed_at timestamptz not null default now()
);

create table if not exists stg.orders_clean (
    order_id bigint primary key,
    customer_id bigint not null,
    order_amount numeric(12, 2) not null,
    order_date date not null,
    order_month text not null,
    currency_code text not null,
    ship_country text not null,
    refund_amount numeric(12, 2) not null,
    net_amount numeric(12, 2) not null,
    etl_processed_at timestamptz not null default now()
);

create table if not exists dw.dim_customers (
    customer_id bigint primary key,
    customer_name text not null,
    city text not null,
    customer_level text not null,
    etl_processed_at timestamptz not null default now()
);

create table if not exists dw.fact_orders (
    order_id bigint primary key,
    customer_id bigint not null references dw.dim_customers(customer_id),
    order_amount numeric(12, 2) not null,
    order_date date not null,
    order_month text not null,
    currency_code text not null,
    ship_country text not null,
    refund_amount numeric(12, 2) not null,
    net_amount numeric(12, 2) not null,
    etl_processed_at timestamptz not null default now()
);

create table if not exists mart.sales_summary (
    summary_key text primary key,
    city text not null,
    customer_level text not null,
    order_month text not null,
    currency_code text not null,
    order_count integer not null,
    gross_sales numeric(12, 2) not null,
    total_refunds numeric(12, 2) not null,
    net_sales numeric(12, 2) not null,
    avg_order_amount numeric(12, 2) not null,
    etl_processed_at timestamptz not null default now()
);

create table if not exists mart.daily_sales (
    order_date date not null,
    order_month text not null,
    currency_code text not null,
    order_count integer not null,
    customer_count integer not null,
    gross_merchandise_value numeric(12, 2) not null,
    total_refunds numeric(12, 2) not null,
    net_sales numeric(12, 2) not null,
    average_order_value numeric(12, 2) not null,
    etl_processed_at timestamptz not null default now(),
    primary key (order_date, currency_code)
);

create table if not exists mart.city_sales (
    city text not null,
    order_month text not null,
    currency_code text not null,
    order_count integer not null,
    customer_count integer not null,
    gross_merchandise_value numeric(12, 2) not null,
    total_refunds numeric(12, 2) not null,
    net_sales numeric(12, 2) not null,
    average_order_value numeric(12, 2) not null,
    etl_processed_at timestamptz not null default now(),
    primary key (city, order_month, currency_code)
);

create table if not exists mart.customer_level_sales (
    customer_level text not null,
    order_month text not null,
    currency_code text not null,
    order_count integer not null,
    customer_count integer not null,
    gross_merchandise_value numeric(12, 2) not null,
    total_refunds numeric(12, 2) not null,
    net_sales numeric(12, 2) not null,
    average_order_value numeric(12, 2) not null,
    etl_processed_at timestamptz not null default now(),
    primary key (customer_level, order_month, currency_code)
);

create index if not exists idx_orders_raw_customer_id on raw.orders_raw(customer_id);
create index if not exists idx_orders_clean_customer_id on stg.orders_clean(customer_id);
create index if not exists idx_fact_orders_customer_id on dw.fact_orders(customer_id);
create index if not exists idx_sales_summary_month on mart.sales_summary(order_month);
create index if not exists idx_daily_sales_month on mart.daily_sales(order_month);
create index if not exists idx_city_sales_month on mart.city_sales(order_month);
create index if not exists idx_customer_level_sales_month on mart.customer_level_sales(order_month);
