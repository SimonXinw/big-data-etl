"""GraphQL Connection 游标分页（通用只读）。"""

from __future__ import annotations

from typing import Any, Iterator

from lib.shopify.client import ShopifyAdminClient


def iter_connection_nodes(
    client: ShopifyAdminClient,
    query: str,
    connection_field: str,
    *,
    page_size: int = 250,
    extra_variables: dict[str, Any] | None = None,
) -> Iterator[dict[str, Any]]:
    """对符合 Relay Connection 形态的字段做游标分页，逐个 yield `node` 字典。

    约定：query 使用变量 `$first: Int!`, `$after: String`，且返回体为
    `{ connection_field: { pageInfo { hasNextPage endCursor }, edges { node { ... } } } }`。
    """

    after: str | None = None

    while True:
        variables: dict[str, Any] = {"first": page_size, "after": after}
        if extra_variables:
            variables.update(extra_variables)

        data = client.execute(query, variables)
        connection = data.get(connection_field)
        if not isinstance(connection, dict):
            break

        for edge in connection.get("edges") or []:
            if not isinstance(edge, dict):
                continue
            node = edge.get("node")
            if isinstance(node, dict):
                yield node

        page_info = connection.get("pageInfo") or {}
        if not isinstance(page_info, dict):
            break

        if not page_info.get("hasNextPage"):
            break

        end_cursor = page_info.get("endCursor")
        if not end_cursor:
            break

        after = str(end_cursor)
