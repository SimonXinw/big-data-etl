"""基于通用 client + pager 的资源遍历（只读）。"""

from __future__ import annotations

from typing import Any, Iterator

from lib.shopify.client import ShopifyAdminClient
from lib.shopify.pager import iter_connection_nodes
from lib.shopify.queries.customers import CUSTOMERS_PAGE_QUERY
from lib.shopify.queries.orders import ORDERS_PAGE_QUERY


def iter_customer_nodes(client: ShopifyAdminClient, *, page_size: int = 250) -> Iterator[dict[str, Any]]:
    """遍历店铺客户（GraphQL node 原始字典）。"""

    yield from iter_connection_nodes(
        client,
        CUSTOMERS_PAGE_QUERY,
        "customers",
        page_size=page_size,
    )


def iter_order_nodes(client: ShopifyAdminClient, *, page_size: int = 250) -> Iterator[dict[str, Any]]:
    """遍历店铺订单（GraphQL node 原始字典）。"""

    yield from iter_connection_nodes(
        client,
        ORDERS_PAGE_QUERY,
        "orders",
        page_size=page_size,
    )
