"""订单列表分页查询（ETL 用金额、客户、时间；可按同格式追加字段）。"""

# GraphQL 变量约定：$first: Int!, $after: String

ORDERS_PAGE_QUERY = """
query OrdersPage($first: Int!, $after: String) {
  orders(first: $first, after: $after, sortKey: CREATED_AT, reverse: true) {
    pageInfo {
      hasNextPage
      endCursor
    }
    edges {
      cursor
      node {
        legacyResourceId
        name
        createdAt
        displayFinancialStatus
        currentTotalPriceSet {
          shopMoney {
            amount
            currencyCode
          }
        }
        customer {
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
}
"""
