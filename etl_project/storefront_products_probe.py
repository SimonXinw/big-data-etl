"""用 Storefront API 分页拉取全部商品（探测网络与凭据）。"""

from __future__ import annotations

from lib.shopify.storefront_client import ShopifyStorefrontClient, read_storefront_config_from_env

# Storefront 单次 `products` 连接最大 first 一般为 250
_PAGE_SIZE = 250
_MAX_PAGES = 200

STOREFRONT_PRODUCTS_PAGE_QUERY = """
query StorefrontProductsPage($first: Int!, $after: String) {
  products(first: $first, after: $after) {
    pageInfo {
      hasNextPage
      endCursor
    }
    edges {
      node {
        id
        title
        handle
      }
    }
  }
}
"""


def run_probe() -> int:
    """执行分页拉取并打印摘要。成功返回 0，失败返回 1。"""

    try:
        config = read_storefront_config_from_env()
    except Exception as exc:  # noqa: BLE001 — 探测入口向用户打印可读错误
        print(f"配置错误: {exc}")
        return 1

    client = ShopifyStorefrontClient(config)
    total = 0
    after: str | None = None
    pages = 0

    print(
        f"Storefront 请求: https://{config.store_domain}/api/{config.api_version}/graphql.json "
        f"(每页最多 {_PAGE_SIZE} 条)"
    )

    try:
        while pages < _MAX_PAGES:
            pages += 1
            data = client.execute(
                STOREFRONT_PRODUCTS_PAGE_QUERY,
                {"first": _PAGE_SIZE, "after": after},
            )
            products = data.get("products")
            if not isinstance(products, dict):
                print("响应结构异常: 缺少 products")
                return 1

            edges = products.get("edges")
            if not isinstance(edges, list):
                print("响应结构异常: products.edges 不是列表")
                return 1

            batch = len(edges)
            total += batch
            page_info = products.get("pageInfo")
            has_next = False
            end_cursor: str | None = None
            if isinstance(page_info, dict):
                has_next = bool(page_info.get("hasNextPage"))
                raw_cursor = page_info.get("endCursor")
                end_cursor = raw_cursor if isinstance(raw_cursor, str) else None

            print(f"第 {pages} 页: 本页 {batch} 条，累计 {total} 条")

            if not has_next or batch == 0:
                break

            after = end_cursor
            if not after:
                break

    except Exception as exc:  # noqa: BLE001
        print(f"请求失败: {exc}")
        return 1

    print(f"完成: 共拉取 {total} 个商品（{pages} 页）。")
    return 0
