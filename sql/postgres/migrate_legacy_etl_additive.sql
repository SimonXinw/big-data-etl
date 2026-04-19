-- 可选：旧版 init 已创建 raw/stg/dw/mart 表、但列少于当前 Python ETL 所需时，用本脚本**仅追加 / 回填列**。
-- 不 DROP 表；不修改 public 等业务 schema。
-- 全新库请只跑 init_warehouse.sql，不必执行本文件。
--
-- mart.big_data_etl_daily_sales 若仍为「单列主键 order_date」旧结构，主键变更请单独评估（本脚本不自动 DROP 约束）。

-- —— raw.big_data_etl_orders_raw ——
alter table raw.big_data_etl_orders_raw add column if not exists currency_code text not null default 'CNY';
alter table raw.big_data_etl_orders_raw add column if not exists ship_country text not null default 'UNKNOWN';
alter table raw.big_data_etl_orders_raw add column if not exists refund_amount numeric(12, 2) not null default 0;

-- —— stg.big_data_etl_orders_clean ——
alter table stg.big_data_etl_orders_clean add column if not exists currency_code text;
alter table stg.big_data_etl_orders_clean add column if not exists ship_country text;
alter table stg.big_data_etl_orders_clean add column if not exists refund_amount numeric(12, 2);
alter table stg.big_data_etl_orders_clean add column if not exists net_amount numeric(12, 2);

update stg.big_data_etl_orders_clean
set
    currency_code = coalesce(currency_code, 'CNY'),
    ship_country = coalesce(nullif(trim(ship_country), ''), 'UNKNOWN'),
    refund_amount = coalesce(refund_amount, 0);

update stg.big_data_etl_orders_clean
set net_amount = order_amount - refund_amount
where net_amount is null;

alter table stg.big_data_etl_orders_clean alter column currency_code set not null;
alter table stg.big_data_etl_orders_clean alter column ship_country set not null;
alter table stg.big_data_etl_orders_clean alter column refund_amount set not null;
alter table stg.big_data_etl_orders_clean alter column net_amount set not null;

-- —— dw.big_data_etl_fact_orders ——
alter table dw.big_data_etl_fact_orders add column if not exists currency_code text;
alter table dw.big_data_etl_fact_orders add column if not exists ship_country text;
alter table dw.big_data_etl_fact_orders add column if not exists refund_amount numeric(12, 2);
alter table dw.big_data_etl_fact_orders add column if not exists net_amount numeric(12, 2);

update dw.big_data_etl_fact_orders
set
    currency_code = coalesce(currency_code, 'CNY'),
    ship_country = coalesce(nullif(trim(ship_country), ''), 'UNKNOWN'),
    refund_amount = coalesce(refund_amount, 0);

update dw.big_data_etl_fact_orders
set net_amount = order_amount - refund_amount
where net_amount is null;

alter table dw.big_data_etl_fact_orders alter column currency_code set not null;
alter table dw.big_data_etl_fact_orders alter column ship_country set not null;
alter table dw.big_data_etl_fact_orders alter column refund_amount set not null;
alter table dw.big_data_etl_fact_orders alter column net_amount set not null;

-- —— mart.big_data_etl_sales_summary：若存在旧列 total_sales，则回填 gross_sales / net_sales ——
alter table mart.big_data_etl_sales_summary add column if not exists currency_code text;
alter table mart.big_data_etl_sales_summary add column if not exists gross_sales numeric(12, 2);
alter table mart.big_data_etl_sales_summary add column if not exists total_refunds numeric(12, 2);
alter table mart.big_data_etl_sales_summary add column if not exists net_sales numeric(12, 2);

do $$
begin
    if exists (
        select 1
        from information_schema.columns
        where table_schema = 'mart'
          and table_name = 'big_data_etl_sales_summary'
          and column_name = 'total_sales'
    ) then
        execute $q$
            update mart.big_data_etl_sales_summary
            set
                currency_code = coalesce(nullif(trim(currency_code), ''), 'CNY'),
                gross_sales = coalesce(gross_sales, total_sales),
                total_refunds = coalesce(total_refunds, 0),
                net_sales = coalesce(net_sales, total_sales - coalesce(total_refunds, 0))
            where gross_sales is null
               or net_sales is null
               or currency_code is null
        $q$;
    else
        execute $q$
            update mart.big_data_etl_sales_summary
            set
                currency_code = coalesce(nullif(trim(currency_code), ''), 'CNY'),
                total_refunds = coalesce(total_refunds, 0)
            where currency_code is null
               or total_refunds is null
        $q$;
    end if;
end $$;

alter table mart.big_data_etl_sales_summary alter column currency_code set not null;
alter table mart.big_data_etl_sales_summary alter column gross_sales set not null;
alter table mart.big_data_etl_sales_summary alter column total_refunds set not null;
alter table mart.big_data_etl_sales_summary alter column net_sales set not null;

-- —— mart.big_data_etl_daily_sales / big_data_etl_city_sales / big_data_etl_customer_level_sales ——
alter table mart.big_data_etl_daily_sales add column if not exists currency_code text;
alter table mart.big_data_etl_daily_sales add column if not exists total_refunds numeric(12, 2);
alter table mart.big_data_etl_daily_sales add column if not exists net_sales numeric(12, 2);

update mart.big_data_etl_daily_sales
set
    currency_code = coalesce(nullif(trim(currency_code), ''), 'CNY'),
    total_refunds = coalesce(total_refunds, 0),
    net_sales = coalesce(net_sales, gross_merchandise_value - coalesce(total_refunds, 0))
where currency_code is null
   or total_refunds is null
   or net_sales is null;

alter table mart.big_data_etl_daily_sales alter column currency_code set not null;
alter table mart.big_data_etl_daily_sales alter column total_refunds set not null;
alter table mart.big_data_etl_daily_sales alter column net_sales set not null;

alter table mart.big_data_etl_city_sales add column if not exists currency_code text;
alter table mart.big_data_etl_city_sales add column if not exists total_refunds numeric(12, 2);
alter table mart.big_data_etl_city_sales add column if not exists net_sales numeric(12, 2);

update mart.big_data_etl_city_sales
set
    currency_code = coalesce(nullif(trim(currency_code), ''), 'CNY'),
    total_refunds = coalesce(total_refunds, 0),
    net_sales = coalesce(net_sales, gross_merchandise_value - coalesce(total_refunds, 0))
where currency_code is null
   or total_refunds is null
   or net_sales is null;

alter table mart.big_data_etl_city_sales alter column currency_code set not null;
alter table mart.big_data_etl_city_sales alter column total_refunds set not null;
alter table mart.big_data_etl_city_sales alter column net_sales set not null;

alter table mart.big_data_etl_customer_level_sales add column if not exists currency_code text;
alter table mart.big_data_etl_customer_level_sales add column if not exists total_refunds numeric(12, 2);
alter table mart.big_data_etl_customer_level_sales add column if not exists net_sales numeric(12, 2);

update mart.big_data_etl_customer_level_sales
set
    currency_code = coalesce(nullif(trim(currency_code), ''), 'CNY'),
    total_refunds = coalesce(total_refunds, 0),
    net_sales = coalesce(net_sales, gross_merchandise_value - coalesce(total_refunds, 0))
where currency_code is null
   or total_refunds is null
   or net_sales is null;

alter table mart.big_data_etl_customer_level_sales alter column currency_code set not null;
alter table mart.big_data_etl_customer_level_sales alter column total_refunds set not null;
alter table mart.big_data_etl_customer_level_sales alter column net_sales set not null;
