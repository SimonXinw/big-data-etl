# 本地开发：data/inbox -> 物化 data/raw -> DuckDB -> data/output。
$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path (Split-Path -Parent $PSCommandPath) "..\..")).Path
Set-Location $repoRoot
python -m etl_project --source inbox --target duckdb
