"""宽表同步：Admin 订单 GraphQL 文档拼装（按 UTC 日历日的 `updated_at` 窗口 + 游标）。

GraphQL 文档本体见 `lib.shopify.queries.orders`（`ORDERS_BY_UPDATED_DAY_TEMPLATE`）。
"""

from __future__ import annotations

from lib.shopify.queries.orders import ORDERS_BY_UPDATED_DAY_TEMPLATE


def build_orders_query(*, search_filter: str, after_cursor: str | None) -> str:
    """组装订单查询请求体（`after` 为空则不带该参数）。

    `search_filter` 为 Shopify Admin `orders` 上 `query` 参数的搜索串；
    按日同步时使用 `updated_at_day_filter` 生成的单日 UTC 窗口。
    """

    after_clause = f', after: "{after_cursor}"' if after_cursor else ""
    body = ORDERS_BY_UPDATED_DAY_TEMPLATE.replace("__AFTER_CLAUSE__", after_clause)
    body = body.replace("__DAY_FILTER__", search_filter)

    return body.strip()


def updated_at_day_filter(day_str: str) -> str:
    """单日 UTC 窗口：`updated_at` 落在该日历日内（与 bi-database `single_day_filter` 一致）。"""

    return f"updated_at:>={day_str}T00:00:00Z updated_at:<={day_str}T23:59:59Z"
