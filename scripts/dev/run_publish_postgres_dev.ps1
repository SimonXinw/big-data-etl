# 本地开发：跑完 DuckDB 分层后，把结果发布到 PostgreSQL / Supabase（给 BI 用）。
# 需要 .env 中 ETL_POSTGRES_DSN；数据库需已执行 sql/postgres/init_warehouse.sql。
param(
    [ValidateSet("csv", "inbox", "shopify")]
    [string] $Source = "csv"
)
$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path (Split-Path -Parent $PSCommandPath) "..\..")).Path
Set-Location $repoRoot
python -m etl_project --source $Source --target postgres
