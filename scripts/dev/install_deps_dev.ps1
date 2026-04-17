# 本地开发：安装 Python 依赖（只需在环境变化后偶尔执行）。
$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path (Split-Path -Parent $PSCommandPath) "..\..")).Path
Set-Location $repoRoot
python -m pip install -r requirements.txt
