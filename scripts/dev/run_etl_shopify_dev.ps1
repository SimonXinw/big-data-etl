# 本地开发：Shopify Admin GraphQL -> data/raw -> DuckDB -> data/output。
# 需要项目根目录 .env 中 SHOPIFY_STORE_DOMAIN、SHOPIFY_ADMIN_ACCESS_TOKEN（旧名 SHOPIFY_ACCESS_TOKEN 仍兼容）。
$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path (Split-Path -Parent $PSCommandPath) "..\..")).Path
Set-Location $repoRoot
python -m etl_project --source shopify --target duckdb
