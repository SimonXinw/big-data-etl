"""订单相关 Admin GraphQL 文档。

- `ORDERS_PAGE_QUERY`：通用 Connection 分页（变量 `$first` / `$after`）；可与自写游标循环或辅助函数配合。
- `ORDERS_BY_UPDATED_DAY_TEMPLATE`：与 bi-database 对齐的宽表同步（按日 `updated_at` 占位符），由 `etl_project.integrations.shopify.admin.orders_bi` 拼装。
"""

# GraphQL 变量约定（分页查询）：$first: Int!, $after: String

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

# 节点字段与 bi-database `shopify_dindan_lishi.get_shop_orders_between_updated_date` 对齐，
# 并补充 legacyResourceId（订单 / 客户）供 ETL 数值主键衍生。
ORDERS_BY_UPDATED_DAY_TEMPLATE = """
{
  orders(
    first: 200
    __AFTER_CLAUSE__
    query: "__DAY_FILTER__"
  ) {
    edges {
      cursor
      node {
        id
        legacyResourceId
        name
        subtotalPriceSet {
          shopMoney {
            amount
            currencyCode
          }
        }
        totalShippingPriceSet {
          shopMoney {
            amount
            currencyCode
          }
        }
        totalDiscountsSet {
          shopMoney {
            amount
            currencyCode
          }
        }
        totalTaxSet {
          shopMoney {
            amount
            currencyCode
          }
        }
        createdAt
        processedAt
        updatedAt
        cancelledAt
        cancelReason
        displayFinancialStatus
        displayFulfillmentStatus
        customerAcceptsMarketing
        currentSubtotalLineItemsQuantity
        currentCartDiscountAmountSet {
          shopMoney {
            amount
            currencyCode
          }
        }
        email
        currencyCode
        discountCode
        phone
        tags
        note
        customer {
          id
          legacyResourceId
          displayName
          email
          phone
          firstName
          lastName
        }
        shippingAddress {
          address1
          address2
          city
          province
          country
          zip
          phone
          name
        }
        billingAddress {
          address1
          address2
          city
          province
          country
          zip
          phone
          name
        }
        lineItems(first: 20) {
          nodes {
            id
            name
            quantity
            currentQuantity
            isGiftCard
            sku
            vendor
            originalTotalSet {
              shopMoney {
                amount
                currencyCode
              }
            }
            product {
              id
              title
            }
            originalUnitPriceSet {
              shopMoney {
                amount
                currencyCode
              }
            }
            discountedTotalSet {
              shopMoney {
                amount
                currencyCode
              }
            }
            discountAllocations {
              allocatedAmountSet {
                shopMoney {
                  amount
                  currencyCode
                }
              }
              discountApplication {
                __typename
                ... on ManualDiscountApplication {
                  title
                }
                ... on AutomaticDiscountApplication {
                  title
                }
                ... on ScriptDiscountApplication {
                  title
                }
              }
            }
            totalDiscountSet {
              shopMoney {
                amount
                currencyCode
              }
            }
          }
        }
        fulfillments(first: 10) {
          trackingInfo {
            number
            url
            company
          }
          status
        }
        totalPriceSet {
          shopMoney {
            amount
            currencyCode
          }
        }
        refunds(first: 10) {
          id
          createdAt
          note
          refundLineItems(first: 10) {
            edges {
              node {
                lineItem {
                  id
                  name
                  variant {
                    id
                    sku
                  }
                  originalUnitPriceSet {
                    shopMoney {
                      amount
                      currencyCode
                    }
                  }
                }
                quantity
              }
            }
          }
          totalRefundedSet {
            shopMoney {
              amount
              currencyCode
            }
          }
        }
        totalRefundedSet {
          shopMoney {
            amount
            currencyCode
          }
        }
        metafields(first: 10) {
          edges {
            node {
              namespace
              key
              value
            }
          }
        }
        transactions(first: 10) {
          id
          kind
          status
          gateway
          createdAt
        }
      }
    }
    pageInfo {
      hasNextPage
    }
  }
}
"""
