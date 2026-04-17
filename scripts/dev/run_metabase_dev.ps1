# 本地开发：启动 Metabase 容器（默认 http://localhost:3000）。需已安装 Docker。
$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path (Split-Path -Parent $PSCommandPath) "..\..")).Path
Set-Location (Join-Path $repoRoot "bi\metabase")
docker compose up -d
