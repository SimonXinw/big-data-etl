"""Shopify 相关异常类型。"""


class ShopifyAdminError(RuntimeError):
    """Shopify Admin 调用失败的基类。"""


class ShopifyConfigError(ValueError):
    """环境变量或域名配置不合法。"""


class ShopifyGraphQLError(ShopifyAdminError):
    """GraphQL 返回 errors 或响应结构异常。"""
