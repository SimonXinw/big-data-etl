-- PostgreSQL / Supabase 初始化脚本
-- 目标：提供一个适合学习 ETL 的最小数仓分层结构
-- 分层：raw -> stg -> dw -> mart

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
    source_system text not null default 'csv_demo',
    etl_loaded_at timestamptz not null default now()
);

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
    etl_processed_at timestamptz not null default now()
);

create table if not exists mart.sales_summary (
    summary_key text primary key,
    city text not null,
    customer_level text not null,
    order_month text not null,
    order_count integer not null,
    total_sales numeric(12, 2) not null,
    avg_order_amount numeric(12, 2) not null,
    etl_processed_at timestamptz not null default now()
);

create table if not exists mart.daily_sales (
    order_date date primary key,
    order_month text not null,
    order_count integer not null,
    customer_count integer not null,
    gross_merchandise_value numeric(12, 2) not null,
    average_order_value numeric(12, 2) not null,
    etl_processed_at timestamptz not null default now()
);

create table if not exists mart.city_sales (
    city text not null,
    order_month text not null,
    order_count integer not null,
    customer_count integer not null,
    gross_merchandise_value numeric(12, 2) not null,
    average_order_value numeric(12, 2) not null,
    etl_processed_at timestamptz not null default now(),
    primary key (city, order_month)
);

create table if not exists mart.customer_level_sales (
    customer_level text not null,
    order_month text not null,
    order_count integer not null,
    customer_count integer not null,
    gross_merchandise_value numeric(12, 2) not null,
    average_order_value numeric(12, 2) not null,
    etl_processed_at timestamptz not null default now(),
    primary key (customer_level, order_month)
);

create index if not exists idx_orders_raw_customer_id on raw.orders_raw(customer_id);
create index if not exists idx_orders_clean_customer_id on stg.orders_clean(customer_id);
create index if not exists idx_fact_orders_customer_id on dw.fact_orders(customer_id);
create index if not exists idx_sales_summary_month on mart.sales_summary(order_month);
create index if not exists idx_daily_sales_month on mart.daily_sales(order_month);
create index if not exists idx_city_sales_month on mart.city_sales(order_month);
create index if not exists idx_customer_level_sales_month on mart.customer_level_sales(order_month);
