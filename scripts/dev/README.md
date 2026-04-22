# 本地开发脚本（`*_dev.ps1`）

仓库**原先没有**「每个流程一个内置 `xxx_dev` 代码副本」；Python 入口统一是 `python -m etl_project`，用参数区分数据源与目标。

本目录提供 **Windows PowerShell 的一键副本**：文件名带 `_dev`，在资源管理器中右键「使用 PowerShell 运行」或在终端里执行均可。脚本会自动 `cd` 到仓库根目录，不依赖你当前所在路径。

## 脚本一览

| 文件 | 作用 |
|------|------|
| `install_deps_dev.ps1` | `pip install -r requirements.txt` |
| `run_etl_csv_dev.ps1` | 默认：`data/raw` CSV → DuckDB → `data/output` |
| `run_etl_inbox_dev.ps1` | `data/inbox` 物化 raw → DuckDB → `data/output` |
| （手动）Storefront 商品 JSON | `python -m etl_project --export-storefront-products-json` → `data/products/storefront_products.json` |
| `run_etl_shopify_dev.ps1` | Shopify 拉数 → raw → DuckDB（需 `.env` 里 Shopify 变量） |
| `run_publish_postgres_dev.ps1` | ETL 并发布到 Postgres；可选参数 `-Source inbox` / `shopify` |
| `serve_datareport.ps1` | 启动 **`datareport`**：`python datareport/serve.py`（默认 `http://127.0.0.1:8787/`） |
| `run_metabase_dev.ps1` | 若仓库中存在 **`bi/metabase`**：`docker compose up -d`；否则请按 **`docs/bi-guide.md`** 自行部署 Metabase |

最短演示（装依赖 + 默认 CSV→DuckDB）：先 `install_deps_dev.ps1`，再 `run_etl_csv_dev.ps1`。

## 使用示例

在仓库根目录：

```powershell
.\scripts\dev\run_etl_csv_dev.ps1
```

带参数发布（例如 inbox 源）：

```powershell
.\scripts\dev\run_publish_postgres_dev.ps1 -Source inbox
```

## 与「代码里的 _dev 副本」的区别

- **DuckDB 文件名、raw 路径** 仍由 `etl_project/config.py` 固定（例如 `etl_learning.duckdb`），没有自动生成 `*_dev.duckdb` 的第二套路径；若要完全隔离环境，可自行复制数据目录或改配置（需改代码）。
- 这些脚本只是 **把推荐命令固化成可双击/可复述的文件**，方便本地跑通。

更完整的步骤说明（含要「执行」的 SQL 文件、BI 浏览器地址）见：`docs/local-full-runbook.md`。

各 `*_dev.ps1` 开头会用 `$PSCommandPath` 自动定位仓库根目录，可从任意当前目录调用。
