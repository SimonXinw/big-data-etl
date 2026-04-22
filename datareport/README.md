# datareport · 分层数据报表

与主文档对齐的**端到端命令**（本地 DuckDB / 发布 Postgres / 启动本服务）见仓库根目录 **`README.md`** 中的 **「命令速查：拉数 → 落库 → 报表」**。

本目录提供一个**独立于 ETL 管线**的轻量报表：选择 `raw / stg / dw / mart` 与物理表，展示 **KPI 指标块 + 明细表 + 可选图表**。技术栈：

- **静态 HTML + 拆分 CSS / JS（ES Module）**：无打包步骤。
- **`serve.py`**：同源提供页面与 `/api/*`。
- **取数路径（自动选择）**
  1. 若 `.env` 配置了 **`SUPABASE_URL` + `SUPABASE_PUBLISHABLE_KEY`（或 `SUPABASE_ANON_KEY`）**：默认走 **Supabase PostgREST（HTTPS 443）**，由 **`/api/supabase-rows`** 在服务端转发请求（密钥不写入仓库 HTML）。
  2. 否则回退 **`/api/rows`**：用 **`ETL_POSTGRES_DSN`** 直连 Postgres（`psycopg`）。
- **可选**：设置 **`DATAREPORT_CLIENT_SUPABASE_FETCH=1`** 时，`/api/client-config` 会把 URL 与 anon 密钥下发给浏览器，由前端 **直连** `*.supabase.co/rest/v1`（仅建议本地调试；密钥会出现在浏览器网络面板）。

> 与 `etl_project` 的关系：表清单白名单与 `etl_project/etl/postgres_loader.py` 中 `PUBLISH_PLANS` 的目标表保持一致；**修改发布表时请同步更新 `public/assets/table_catalog.json`**（含 `kpis`、`chart` 元数据）。

## 为什么直连 Postgres 会失败，而 Supabase REST 可以？

部分网络环境（透明代理 / 抓包工具 / VPN）会把 `*.pooler.supabase.com` 或 `db.*.supabase.co:5432` 解析到 **`198.18.x.x`** 等保留段，导致 `psycopg` 报 *server closed the connection unexpectedly*。  
**PostgREST 走 HTTPS 443**，通常仍可按公网正常 TLS 访问 `https://<project>.supabase.co`，因此本仓库**优先**使用 Supabase REST 路径读数。

## 1. 前置条件

1. 已在目标库执行 `sql/postgres/init_warehouse.sql`。
2. 项目根 `.env` 至少配置以下**之一**：
   - **推荐（报表读数）**：`SUPABASE_URL`、`SUPABASE_PUBLISHABLE_KEY`（或 `SUPABASE_ANON_KEY`）。
   - **ETL 发布仍可用**：`ETL_POSTGRES_DSN`（若 5432 在你机器上不可用，可继续用 CI/服务器跑 `python -m etl_project --target postgres`，报表侧只依赖 Supabase REST）。
3. **Supabase Dashboard → Settings → API**：在 **Exposed schemas** 中加入 `raw,stg,dw,mart`。
4. 在 SQL Editor 执行 **`sql/postgres/supabase_grants_for_datareport.sql`**（授予 `anon/authenticated` 的 `SELECT` 与宽松 RLS 读策略；仅建议学习/演示环境）。
5. 已发布数据，例如：

```bash
python -m etl_project --source inbox --target postgres
```

## 2. 启动报表服务

```bash
python datareport/serve.py
```

默认 `http://127.0.0.1:8787/`。Windows：`.\scripts\dev\serve_datareport.ps1`

## 3. HTTP API（只读）

| 路径 | 说明 |
|------|------|
| `GET /api/health` | `postgres_configured`、`supabase_configured`、`default_data_path` 等 |
| `GET /api/client-config` | 前端策略配置；若开启 `DATAREPORT_CLIENT_SUPABASE_FETCH`，额外返回 `supabase_url` / `supabase_anon_key` |
| `GET /api/catalog` | `table_catalog.json` |
| `GET /api/supabase-rows?schema=mart&table=big_data_etl_daily_sales&limit=500` | 服务端持 anon 密钥访问 PostgREST |
| `GET /api/rows?...` | `psycopg` 直连（回退路径） |

行数上限：`serve.py` 中 `MAX_ROWS`（默认 2000）。

## 4. 目录结构

```text
datareport/
├─ README.md
├─ serve.py
└─ public/
   ├─ index.html
   └─ assets/
      ├─ table_catalog.json   # 表、分层、kpis、chart
      ├─ css/report.css
      └─ js/
         ├─ app.js
         ├─ api.js
         ├─ metrics.js         # KPI 块
         ├─ renderTable.js
         └─ chartPanel.js
```

## 5. 安全说明

- **不要把 `SUPABASE_PUBLISHABLE_KEY` 硬编码进提交到 Git 的 HTML**；默认使用同源 `/api/supabase-rows`，密钥只留在 `.env`。
- 若密钥曾出现在公开聊天或截图，请在 Supabase 控制台**轮换** anon / publishable key。
- `serve.py` 默认仅监听本机；生产请改为受控网关 + 严格 RLS。

## 6. 为何不用 React / npm 打包？

与主项目「纯 Python、零 Node 构建」一致；需要组件化时可再引入 Vite + React。
