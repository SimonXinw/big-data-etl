# 本地开发：data/raw CSV -> DuckDB -> data/output（默认学习链路）。
$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path (Split-Path -Parent $PSCommandPath) "..\..")).Path
Set-Location $repoRoot
python -m etl_project --source csv --target duckdb
