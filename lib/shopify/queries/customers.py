"""客户列表分页查询（字段保持 ETL 所需最小集，可按同格式扩展）。"""

# GraphQL 变量约定：$first: Int!, $after: String

CUSTOMERS_PAGE_QUERY = """
query CustomersPage($first: Int!, $after: String) {
  customers(first: $first, after: $after) {
    pageInfo {
      hasNextPage
      endCursor
    }
    edges {
      cursor
      node {
        legacyResourceId
        firstName
        lastName
        email
        tags
        defaultAddress {
          city
        }
      }
    }
  }
}
"""
