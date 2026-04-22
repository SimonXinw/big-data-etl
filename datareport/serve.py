"""datareport：静态报表 + 只读数据 API（开发用）。

设计目标
--------
- **不改 ETL 逻辑**：本文件独立在 `datareport/`，读取 `ETL_POSTGRES_DSN` 与/或 `SUPABASE_*`。
- **同源一体**：单端口同时提供 `index.html` 与 `/api/*`，避免浏览器 CORS。
- **白名单查表**：仅允许 `public/assets/table_catalog.json` 中声明的 schema.table。
- **Supabase 优先路径**：若配置了 `SUPABASE_URL` + `SUPABASE_PUBLISHABLE_KEY`（或 `SUPABASE_ANON_KEY`），
  默认通过 **PostgREST（HTTPS 443）** 拉数，绕开部分环境下直连 Postgres `5432` 被代理劫持的问题。

安全提示
--------
- 默认只绑定 `127.0.0.1`，适合本机开发；**不要**把带生产凭据的 DSN 暴露在公网。
- Postgres 路径仍是只读 `SELECT`；Supabase 路径使用 **publishable / anon** 密钥，请配合 RLS 与最小权限。

用法::

    python datareport/serve.py
    python datareport/serve.py --port 8787

PowerShell 快捷脚本见 `scripts/dev/serve_datareport.ps1`。
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from decimal import Decimal
from datetime import date, datetime
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
DATAREPORT_ROOT = Path(__file__).resolve().parent

_repo_root_str = str(REPO_ROOT)

if _repo_root_str not in sys.path:
    sys.path.insert(0, _repo_root_str)
PUBLIC_DIR = DATAREPORT_ROOT / "public"
CATALOG_PATH = PUBLIC_DIR / "assets" / "table_catalog.json"

MAX_ROWS = 2000
DEFAULT_ROWS = 500


def _load_catalog() -> dict:
    with CATALOG_PATH.open(encoding="utf-8") as handle:
        return json.load(handle)


def _build_allowlist(catalog: dict) -> set[tuple[str, str]]:
    pairs: set[tuple[str, str]] = set()

    for layer in catalog.get("layers", []):
        for table in layer.get("tables", []):
            schema = str(table.get("schema", "")).strip()
            name = str(table.get("name", "")).strip()

            if schema and name:
                pairs.add((schema, name))

    return pairs


CATALOG = _load_catalog()
ALLOWLIST = _build_allowlist(CATALOG)


def _read_supabase_url() -> str:
    return os.getenv("SUPABASE_URL", "").strip().rstrip("/")


def _read_supabase_anon_key() -> str:
    return (
        os.getenv("SUPABASE_PUBLISHABLE_KEY", "").strip()
        or os.getenv("SUPABASE_ANON_KEY", "").strip()
    )


def _supabase_ready() -> bool:
    return bool(_read_supabase_url() and _read_supabase_anon_key())


def _default_data_path() -> str:
    if _supabase_ready():
        return "supabase_rest"

    if _read_postgres_dsn():
        return "postgres_direct"

    return "none"


def _client_supabase_fetch_enabled() -> bool:
    flag = os.getenv("DATAREPORT_CLIENT_SUPABASE_FETCH", "").strip().lower()

    return flag in ("1", "true", "yes", "on")


def _fetch_supabase_rest_rows(url: str, anon_key: str, schema: str, table: str, limit: int) -> dict[str, object]:
    """通过 Supabase PostgREST（HTTPS 443）拉数，绕开本机直连 Postgres 5432 的常见代理问题。"""

    safe_table = urllib.parse.quote(table, safe="")
    rest_url = f"{url.rstrip('/')}/rest/v1/{safe_table}?select=*&limit={int(limit)}"
    request_obj = urllib.request.Request(
        rest_url,
        headers={
            "apikey": anon_key,
            "Authorization": f"Bearer {anon_key}",
            "Accept": "application/json",
            "Accept-Profile": schema,
        },
        method="GET",
    )

    try:
        with urllib.request.urlopen(request_obj, timeout=90) as response:
            raw = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")

        try:
            parsed = json.loads(detail)
            message = str(parsed.get("message") or parsed.get("error") or detail)
        except json.JSONDecodeError:
            message = detail or str(exc)

        if "invalid schema" in message.lower():
            message = (
                f"{message} "
                "（请在 Supabase Dashboard → Settings → API →「Exposed schemas」中加入 "
                "raw, stg, dw, mart 并保存；仅靠 GRANT SQL 无法消除本条 PostgREST 校验。）"
            )

        raise RuntimeError(message) from exc

    payload = json.loads(raw)

    if not isinstance(payload, list):
        raise RuntimeError("Supabase REST 返回了非 JSON 数组，请检查表是否在对应 schema 暴露给 PostgREST。")

    rows: list[dict[str, object]] = payload
    columns = list(rows[0].keys()) if rows else []

    return {
        "schema": schema,
        "table": table,
        "limit": limit,
        "row_count": len(rows),
        "columns": columns,
        "rows": rows,
        "source": "supabase_rest",
    }


def _read_postgres_dsn() -> str:
    from etl_project.config import read_postgres_dsn

    return read_postgres_dsn().strip()


def _json_bytes(payload: object, status: HTTPStatus = HTTPStatus.OK) -> tuple[int, bytes, str]:
    body = json.dumps(payload, ensure_ascii=False, default=_json_default).encode("utf-8")

    return status.value, body, "application/json; charset=utf-8"


def _json_default(value: object) -> object:
    if isinstance(value, (datetime, date)):
        return value.isoformat()

    if isinstance(value, Decimal):
        return float(value)

    return str(value)


def _parse_limit(raw: str | None) -> int:
    if not raw:
        return DEFAULT_ROWS

    try:
        parsed = int(raw)
    except ValueError:
        return DEFAULT_ROWS

    if parsed < 1:
        return 1

    return min(parsed, MAX_ROWS)


class DatareportRequestHandler(SimpleHTTPRequestHandler):
    """提供静态资源与只读 API。"""

    protocol_version = "HTTP/1.1"

    def __init__(self, request, client_address, server) -> None:  # noqa: D417
        super().__init__(request, client_address, server, directory=str(PUBLIC_DIR))

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        sys.stderr.write("%s - - [%s] %s\n" % (self.address_string(), self.log_date_time_string(), format % args))

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)

        if parsed.path == "/api/catalog":
            self._send_json_bytes(*_json_bytes(CATALOG))
            return

        if parsed.path == "/api/health":
            dsn_configured = bool(_read_postgres_dsn())
            supabase_ok = _supabase_ready()
            default_path = _default_data_path()

            payload = {
                "ok": True,
                "postgres_configured": dsn_configured,
                "supabase_configured": supabase_ok,
                "client_supabase_fetch_enabled": _client_supabase_fetch_enabled(),
                "default_data_path": default_path,
                "catalog_version": CATALOG.get("version"),
            }
            self._send_json_bytes(*_json_bytes(payload))
            return

        if parsed.path == "/api/client-config":
            url = _read_supabase_url()
            key = _read_supabase_anon_key()
            expose = _client_supabase_fetch_enabled()
            payload: dict[str, object] = {
                "catalog_version": CATALOG.get("version"),
                "postgres_configured": bool(_read_postgres_dsn()),
                "supabase_configured": bool(url and key),
                "client_supabase_fetch_enabled": expose,
                "default_data_path": _default_data_path(),
            }

            if expose and url and key:
                payload["supabase_url"] = url
                payload["supabase_anon_key"] = key

            self._send_json_bytes(*_json_bytes(payload))
            return

        if parsed.path == "/api/supabase-rows":
            self._handle_supabase_rows(parsed.query)
            return

        if parsed.path == "/api/rows":
            self._handle_rows(parsed.query)
            return

        super().do_GET()

    def _send_json_bytes(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _handle_rows(self, query: str) -> None:
        params = parse_qs(query)
        schema = (params.get("schema") or [""])[0].strip()
        table = (params.get("table") or [""])[0].strip()
        limit = _parse_limit((params.get("limit") or [None])[0])

        if (schema, table) not in ALLOWLIST:
            self._send_json_bytes(
                *_json_bytes({"error": "不在白名单的表", "schema": schema, "table": table}, HTTPStatus.BAD_REQUEST)
            )
            return

        dsn = _read_postgres_dsn()

        if not dsn:
            self._send_json_bytes(
                *_json_bytes({"error": "缺少 ETL_POSTGRES_DSN"}, HTTPStatus.SERVICE_UNAVAILABLE)
            )
            return

        try:
            import psycopg
            from psycopg import sql
        except ImportError:
            self._send_json_bytes(
                *_json_bytes({"error": "未安装 psycopg"}, HTTPStatus.INTERNAL_SERVER_ERROR)
            )
            return

        statement = sql.SQL("SELECT * FROM {}.{} LIMIT {}").format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.Literal(limit),
        )

        try:
            with psycopg.connect(dsn) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(statement)
                    column_names = [column.name for column in cursor.description or []]
                    rows = cursor.fetchall()
                    payload = {
                        "schema": schema,
                        "table": table,
                        "limit": limit,
                        "row_count": len(rows),
                        "columns": column_names,
                        "rows": [dict(zip(column_names, row)) for row in rows],
                        "source": "postgres_direct",
                    }
        except Exception as exc:  # noqa: BLE001 - 开发服务器：返回可读错误
            self._send_json_bytes(
                *_json_bytes({"error": str(exc), "schema": schema, "table": table}, HTTPStatus.BAD_GATEWAY)
            )
            return

        self._send_json_bytes(*_json_bytes(payload))

    def _handle_supabase_rows(self, query: str) -> None:
        params = parse_qs(query)
        schema = (params.get("schema") or [""])[0].strip()
        table = (params.get("table") or [""])[0].strip()
        limit = _parse_limit((params.get("limit") or [None])[0])

        if (schema, table) not in ALLOWLIST:
            self._send_json_bytes(
                *_json_bytes({"error": "不在白名单的表", "schema": schema, "table": table}, HTTPStatus.BAD_REQUEST)
            )
            return

        url = _read_supabase_url()
        key = _read_supabase_anon_key()

        if not url or not key:
            self._send_json_bytes(
                *_json_bytes(
                    {"error": "缺少 SUPABASE_URL 或 SUPABASE_PUBLISHABLE_KEY / SUPABASE_ANON_KEY"},
                    HTTPStatus.SERVICE_UNAVAILABLE,
                )
            )
            return

        try:
            payload = _fetch_supabase_rest_rows(url, key, schema, table, limit)
        except Exception as exc:  # noqa: BLE001
            self._send_json_bytes(
                *_json_bytes({"error": str(exc), "schema": schema, "table": table}, HTTPStatus.BAD_GATEWAY)
            )
            return

        self._send_json_bytes(*_json_bytes(payload))


def main() -> None:
    parser = argparse.ArgumentParser(description="启动 datareport 静态站 + 只读 API")
    parser.add_argument("--host", default="127.0.0.1", help="监听地址，默认仅本机")
    parser.add_argument("--port", type=int, default=8787, help="监听端口")
    args = parser.parse_args()

    load_dotenv(REPO_ROOT / ".env", encoding="utf-8-sig", override=True)

    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except OSError:
            pass

    server = ThreadingHTTPServer((args.host, args.port), DatareportRequestHandler)

    print(f"datareport 服务已启动: http://{args.host}:{args.port}/")
    print("按 Ctrl+C 停止")
    print(f"静态目录: {PUBLIC_DIR}")
    print(f"PostgreSQL 已配置 DSN: {bool(_read_postgres_dsn())}")
    print(f"Supabase REST 已配置 URL+Key: {_supabase_ready()}")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n已停止")


if __name__ == "__main__":
    main()
