# DataEase 本地（Docker）

同目录 `docker-compose.yml` 启动：DataEase 2.x + **内置 MariaDB**（只存 DataEase 自己的元数据，**不是** ETL 的 Supabase 业务库）。

## 启停

在**本目录**执行：

```bash
docker compose up -d
docker compose down
```

浏览器：<http://localhost:8100>（端口见 `docker-compose.yml` 中 `ports`）

## 登录 DataEase 网页（管理员）

| 项 | 值（社区镜像常见默认，以你安装后提示为准） |
|----|---------------------------------------------|
| 用户名 | `admin` |
| 密码 | `DataEase@123456` |

首次登录后请**在系统里改密**；与 `docker-compose` 里 **MariaDB 的 root 密码不是同一个东西**。

## `docker-compose` 里写死的 MariaDB 是干什么的

| 项 | 值（本仓库学习用写死，见 yml） |
|----|--------------------------------|
| 用途 | 仅 **DataEase 应用**连自己的元数据库（用户/仪表/配置等） |
| 库名 | `dataease` |
| 用户 / 口令 | 与 yml 中 `MARIADB_ROOT_PASSWORD`、`dataease` 服务的 `DB_PASSWORD` 一致（`root` + 本机开发口令） |

**不要**用这里去连你的 **Supabase / 分析用 PostgreSQL**；业务库在 DataEase 里：**数据源 → 新建 → PostgreSQL**。

## 改业务库（Supabase）连哪

在 **DataEase 界面** 里配置，或把 `ETL_POSTGRES_DSN` 从项目根目录 `.env` 里拆成「主机、端口、库、用户、密码」填进数据源。**不必**写进 `docker-compose.yml`。

## Supabase 连不上 / `Failed to get connection`

DataEase 跑在 **Docker** 里时用容器网络；本机能连不代表容器能连（常见：缺 SSL、代理 Fake-IP、直连不如连接池稳）。

1. **SSL**：额外 JDBC 串加 `sslmode=require`，或在「JDBC 连接」模式粘贴完整 URL（见下）。
2. **优先 Session pooler**：Supabase → Database → Connect → **Session pool**，端口多为 **6543**，用户 **`postgres.<项目 ref>`**。
3. **JDBC 一整条**（减少漏项）：把控制台 URI 改成 JDBC 形式，例如  
   `jdbc:postgresql://主机:端口/postgres?sslmode=require`
4. **代理**：退出 Clash/TUN，`ipconfig /flushdns`，重启 Docker；容器内解析到 **`198.18.x.x`** 说明 DNS 仍异常。
5. **应急**：Supabase SQL Editor 导出 CSV，用 DataEase 静态/Excel 数据源（非实时）。

## 持久化目录

本目录下 `de_data/`：删掉 `de_data/mariadb` 会清空内置库（DataEase 需重建初始化）；请勿误删其他运行中数据。
