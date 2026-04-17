"""GraphQL 查询字符串（只读）。按需在此目录新增 query 文件并保持同样风格。"""

from lib.shopify.queries.customers import CUSTOMERS_PAGE_QUERY
from lib.shopify.queries.orders import ORDERS_PAGE_QUERY

__all__ = ["CUSTOMERS_PAGE_QUERY", "ORDERS_PAGE_QUERY"]
