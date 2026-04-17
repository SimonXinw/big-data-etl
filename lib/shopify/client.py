"""Shopify Admin GraphQL 通用执行客户端（只读 POST）。"""

from __future__ import annotations

from typing import Any, cast

import requests

from lib.shopify.config import ShopifyAdminConfig
from lib.shopify.errors import ShopifyGraphQLError


def _first_graphql_statement(document: str) -> str:
    """取文档中第一条非空、非 `#` 注释行的前缀（用于粗判 query / mutation）。"""

    for raw_line in document.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        return line

    return ""


class ShopifyAdminClient:
    """对 `https://{shop}/admin/api/{version}/graphql.json` 的薄封装。"""

    def __init__(self, config: ShopifyAdminConfig) -> None:
        self._config = config

    @property
    def config(self) -> ShopifyAdminConfig:
        return self._config

    def execute(self, query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
        """执行 GraphQL **query**（只读），返回顶层 `data` 字典。

        显式拒绝 `mutation` / `subscription`，避免误用同一入口做写操作。
        """

        first_statement = _first_graphql_statement(query)
        lowered = first_statement.casefold()
        if lowered.startswith("mutation") or lowered.startswith("subscription"):
            raise ShopifyGraphQLError(
                "本客户端仅允许只读 query：检测到 mutation 或 subscription，已拒绝执行。"
            )

        url = f"https://{self._config.store_domain}/admin/api/{self._config.api_version}/graphql.json"
        response = requests.post(
            url,
            headers={
                "Content-Type": "application/json",
                "X-Shopify-Access-Token": self._config.access_token,
                "Accept": "application/json",
            },
            json={"query": query, "variables": variables or {}},
            timeout=120,
        )

        if response.status_code >= 400:
            raise ShopifyGraphQLError(
                f"Shopify HTTP {response.status_code}: {response.text[:800]}"
            )

        payload = response.json()
        if not isinstance(payload, dict):
            raise ShopifyGraphQLError("Shopify 响应不是 JSON 对象。")

        errors = payload.get("errors")
        if errors:
            raise ShopifyGraphQLError(f"Shopify GraphQL errors: {errors!s}"[:2000])

        data = payload.get("data")
        if not isinstance(data, dict):
            raise ShopifyGraphQLError("Shopify 响应缺少 data 字段。")

        return cast(dict[str, Any], data)
