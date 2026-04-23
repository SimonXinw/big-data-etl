# 本地 Airflow（Docker）

基于 [Apache Airflow 官方 docker-compose](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/)（2.10.4 + CeleryExecutor），为本仓库增加了：

- 扩展镜像：安装根目录 `requirements.txt`
- 将仓库根挂载到容器内 `/opt/etl`，并设置 `ETL_PROJECT_ROOT=/opt/etl`，供 `dags/python_etl_dag.py` 执行 `python -m etl_project`
- 关闭示例 DAG（`LOAD_EXAMPLES=false`）

## 前置条件

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)（Windows 建议启用 WSL2 后端）
- 机器内存建议 ≥ 4GB（官方要求）

## 启动步骤

在 **`airflow` 目录下**执行（使 `AIRFLOW_PROJ_DIR=.` 对应本目录的 `dags/`、`logs/` 等）：

```powershell
cd airflow
copy .env.example .env
docker compose build
docker compose up -d
```

浏览器打开：**http://localhost:8080**

### Web 登录（默认）

| 项 | 值 |
|----|-----|
| 地址 | http://localhost:8080 |
| 用户名 | `airflow` |
| 密码 | `airflow` |

与 `airflow/.env.example`（复制为 `.env`）里的 `_AIRFLOW_WWW_USER_USERNAME`、`_AIRFLOW_WWW_USER_PASSWORD` 一致；**仅在首次 `airflow-init` 创建管理员时生效**，若你已初始化过库，改 `.env` 不会自动改已有密码，需在 UI 里改或重建卷。

首次启动会执行 `airflow-init`，需等待约 1～2 分钟再访问 UI。

## 常用命令

```powershell
docker compose ps
docker compose logs -f airflow-webserver
docker compose down
```

## 与 ETL 的衔接

- DAG 文件：`dags/python_etl_dag.py`
- 容器内工作目录为仓库根，可读写挂载的 `data/`（注意根目录 `.gitignore` 已忽略 `data/*`）
- 若需连 PostgreSQL，在 `airflow/.env` 或宿主环境中设置 `ETL_POSTGRES_DSN` 后重新 `docker compose up -d`

## Linux 提示

若 `logs/` 属主变成 root，请在 `airflow/.env` 中设置 `AIRFLOW_UID` 为本机用户 id（`id -u`），删除旧 `logs` 后重新 `docker compose up`。

## Docker Hub 拉取失败（`redis` / `postgres` / `oauth token` / `EOF`）

报错里出现 `failed to fetch oauth token`、`auth.docker.io`、`EOF`，一般是**访问 Docker Hub 不稳定或被拦**（网络、公司防火墙、地区线路），不是本仓库 compose 写错。

按顺序试：

1. **重试**：偶发断线时多执行几次 `docker compose up -d`。
2. **Docker Desktop 配镜像加速**（国内常用）：**Settings → Docker Engine**，在 JSON 里增加 `registry-mirrors`（地址用你在云厂商控制台拿到的专属加速域名，例如阿里云「容器镜像服务 → 镜像加速器」），**Apply & Restart** 后再 `docker compose up -d`。官方说明见 [Docker registry mirrors](https://docs.docker.com/docker-hub/image-mirror/)。
3. **代理 / VPN**：若本机翻墙才能稳定访问外网，在 Docker Desktop **Settings → Resources → Proxies** 中为 Docker 配置与系统一致的代理。
4. **先单独拉镜像**（便于看清是哪一步失败）：
   ```powershell
   docker pull redis:7.2-bookworm
   docker pull postgres:13
   docker compose up -d
   ```
5. **登录 Docker Hub**（缓解匿名拉取限流，需自行注册账号）：
   ```powershell
   docker login
   ```
