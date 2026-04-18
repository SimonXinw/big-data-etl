"""Storefront API：商品全量分页查询（与 `lib.shopify.storefront` 中遍历逻辑配套）。"""

STOREFRONT_PRODUCTS_FULL_PAGE_QUERY = """
query StorefrontProductsFullPage($first: Int!, $after: String) {
  products(first: $first, after: $after, sortKey: TITLE) {
    pageInfo {
      hasNextPage
      endCursor
    }
    edges {
      cursor
      node {
        id
        title
        handle
        description
        vendor
        productType
        tags
        createdAt
        updatedAt
        onlineStoreUrl
        featuredImage {
          url
          altText
          width
          height
        }
        priceRange {
          minVariantPrice {
            amount
            currencyCode
          }
          maxVariantPrice {
            amount
            currencyCode
          }
        }
        compareAtPriceRange {
          minVariantPrice {
            amount
            currencyCode
          }
          maxVariantPrice {
            amount
            currencyCode
          }
        }
        variants(first: 250) {
          edges {
            node {
              id
              title
              sku
              availableForSale
              price {
                amount
                currencyCode
              }
              compareAtPrice {
                amount
                currencyCode
              }
            }
          }
        }
        images(first: 20) {
          edges {
            node {
              url
              altText
              width
              height
            }
          }
        }
      }
    }
  }
}
"""
