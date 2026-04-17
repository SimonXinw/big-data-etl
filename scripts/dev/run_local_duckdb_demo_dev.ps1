# 本地开发：一键演示「仅 DuckDB」最短路径（安装依赖 + 默认 CSV ETL）。
$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path (Split-Path -Parent $PSCommandPath) "..\..")).Path
Set-Location $repoRoot
python -m pip install -r requirements.txt
python -m etl_project --source csv --target duckdb
Write-Host "完成。产物: data/warehouse/etl_learning.duckdb , data/output/"
