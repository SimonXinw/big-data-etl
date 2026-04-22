# 启动 datareport：静态页面 + 只读 PostgreSQL API（默认 127.0.0.1:8787）
param(
    [int] $Port = 8787
)
$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path (Split-Path -Parent $PSCommandPath) "..\..")).Path
Set-Location $repoRoot
python datareport/serve.py --port $Port
