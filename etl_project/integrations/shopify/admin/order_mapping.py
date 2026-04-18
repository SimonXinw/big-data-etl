"""将 Shopify Admin GraphQL Order node 映射为与 bi-database `shopify_orders` 对齐的宽表行。"""

from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any

import pytz
from dateutil import parser as date_parser

_GUEST_CUSTOMER_OFFSET = 5_000_000_000_000

SHOPIFY_ORDER_COLUMNS = (
    "shopify_gid",
    "shop_name",
    "order_id",
    "legacy_order_id",
    "customer_legacy_id",
    "created_at",
    "beijing_created_at",
    "use_created_at",
    "berlin_created_at",
    "order_status",
    "product_quantity",
    "total_product_amount",
    "shippingfee",
    "total_product_discount",
    "discountfee",
    "tax",
    "logistics_status",
    "marketing_intention",
    "currency_code",
    "total_price",
    "product_sales",
    "totalrefunded",
    "discount_method",
    "customer_display_name",
    "customer_email",
    "customer_phone",
    "bill_country",
    "shipping_address1",
    "shipping_address2",
    "shipping_city",
    "shipping_province",
    "shipping_country",
    "shipping_zip",
    "shipping_phone",
    "shipping_name",
    "discount_code",
    "updated_at",
    "cancelled_at",
    "cancel_reason",
    "product_details",
    "discount_information",
    "billing_address",
    "refunds",
    "etl_synced_at",
)


def _legacy_int_from_gid(gid: str) -> int | None:
    match = re.search(r"/(\d+)\s*$", gid or "")
    if not match:
        return None

    try:
        return int(match.group(1))
    except ValueError:
        return None


def _format_ts(dt: Any) -> str | None:
    if dt is None:
        return None

    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _parse_iso(ts: str | None) -> datetime | None:
    if not ts:
        return None

    parsed = date_parser.isoparse(ts)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=pytz.UTC)

    return parsed


def _money_amount(payload: dict[str, Any], *path: str) -> float:
    cur: Any = payload
    for key in path:
        if not isinstance(cur, dict):
            return 0.0
        cur = cur.get(key)

    if isinstance(cur, dict):
        amt = cur.get("amount")
        if amt is not None:
            return float(amt)

    return 0.0


def graph_order_node_to_row(node: dict[str, Any], *, shop_name: str) -> dict[str, Any]:
    """单条 GraphQL `node`（Order） -> 宽表字典（键为 SQL 列名）。"""

    order = node
    shopify_gid = str(order.get("id") or "")
    legacy_rid = order.get("legacyResourceId")
    try:
        legacy_order_id = int(legacy_rid) if legacy_rid is not None else None
    except (TypeError, ValueError):
        legacy_order_id = None

    if legacy_order_id is None:
        legacy_order_id = _legacy_int_from_gid(shopify_gid)

    created_raw = order.get("createdAt")
    dt_utc = _parse_iso(created_raw) if created_raw else None
    if dt_utc:
        china = pytz.timezone("Asia/Shanghai")
        ny = pytz.timezone("America/New_York")
        berlin = pytz.timezone("Europe/Berlin")
        beijing_created_at = dt_utc.astimezone(china)
        use_created_at = dt_utc.astimezone(ny)
        berlin_created_at = dt_utc.astimezone(berlin)
    else:
        beijing_created_at = None
        use_created_at = None
        berlin_created_at = None

    currency_code = str(order.get("currencyCode") or "")
    product_quantity = int(order.get("currentSubtotalLineItemsQuantity") or 0)

    line_blob = order.get("lineItems") if isinstance(order.get("lineItems"), dict) else {}
    line_nodes = line_blob.get("nodes") if isinstance(line_blob.get("nodes"), list) else []
    product_details = line_nodes

    discount_information: list[dict[str, Any]] = []
    for item in line_nodes:
        if not isinstance(item, dict):
            continue

        item_info: dict[str, Any] = {
            "name": item.get("name"),
            "sku": item.get("sku"),
            "quantity": item.get("quantity"),
            "discounts": [],
        }

        for alloc in item.get("discountAllocations") or []:
            if not isinstance(alloc, dict):
                continue

            da = alloc.get("discountApplication") if isinstance(alloc.get("discountApplication"), dict) else {}
            discount_info = {
                "type": da.get("__typename"),
                "title": da.get("title"),
                "code": da.get("code"),
                "amount": _money_amount(alloc, "allocatedAmountSet", "shopMoney"),
                "currency": (
                    (alloc.get("allocatedAmountSet") or {})
                    .get("shopMoney", {})
                    .get("currencyCode")
                ),
            }
            item_info["discounts"].append(discount_info)

        discount_information.append(item_info)

    billing_address_payload = order.get("billingAddress")
    billing_dump = billing_address_payload if isinstance(billing_address_payload, dict) else {}

    totalrefunded = _money_amount(order, "totalRefundedSet", "shopMoney")

    total_price = _money_amount(order, "totalPriceSet", "shopMoney")
    product_sales = _money_amount(order, "subtotalPriceSet", "shopMoney")
    shippingfee = _money_amount(order, "totalShippingPriceSet", "shopMoney")
    discountfee = _money_amount(order, "totalDiscountsSet", "shopMoney")
    tax = _money_amount(order, "totalTaxSet", "shopMoney")

    exclude_skus = {"DG-002", "DG-004", "TBFS-01"}
    total_product_amount = 0.0
    total_product_discount = 0.0

    for item in line_nodes:
        if not isinstance(item, dict):
            continue

        sku = item.get("sku") or ""
        if sku in exclude_skus:
            continue

        qty = int(item.get("quantity") or 0)
        unit = _money_amount(item, "originalUnitPriceSet", "shopMoney")
        disc = _money_amount(item, "totalDiscountSet", "shopMoney")
        total_product_amount += unit * qty
        total_product_discount += disc * qty

    marketing_intention = 1 if order.get("customerAcceptsMarketing") else 0
    order_status = str(order.get("displayFinancialStatus") or "")
    logistics_status = str(order.get("displayFulfillmentStatus") or "")
    discount_method = ""

    merchant_order_number = str(order.get("name") or "")

    customer = order.get("customer") if isinstance(order.get("customer"), dict) else {}
    cust_legacy = customer.get("legacyResourceId")
    try:
        customer_legacy_id = int(cust_legacy) if cust_legacy is not None else None
    except (TypeError, ValueError):
        customer_legacy_id = None

    if (customer_legacy_id is None or customer_legacy_id == 0) and legacy_order_id is not None:
        customer_legacy_id = _GUEST_CUSTOMER_OFFSET + int(legacy_order_id)

    customer_display_name = (
        str(customer.get("displayName") or "").strip()
        or f'{str(customer.get("firstName") or "").strip()} {str(customer.get("lastName") or "").strip()}'.strip()
    )

    customer_email = str(customer.get("email") or "")
    customer_phone = str(customer.get("phone") or "")

    billcountry = order.get("billingAddress") if isinstance(order.get("billingAddress"), dict) else {}
    bill_country = str(billcountry.get("country") or "")

    shipping_address = order.get("shippingAddress") if isinstance(order.get("shippingAddress"), dict) else {}
    shipping_address1 = str(shipping_address.get("address1") or "")
    shipping_address2 = str(shipping_address.get("address2") or "")
    shipping_city = str(shipping_address.get("city") or "")
    shipping_province = str(shipping_address.get("province") or "")
    shipping_country = str(shipping_address.get("country") or "")
    shipping_zip = str(shipping_address.get("zip") or "")
    shipping_phone = str(shipping_address.get("phone") or "")
    ship_name = str(shipping_address.get("name") or "").strip()

    discount_code_val = order.get("discountCode")
    discount_code = "" if discount_code_val is None else str(discount_code_val)

    updated_raw = order.get("updatedAt")
    updated_dt = _parse_iso(updated_raw) if updated_raw else None

    cancelled_raw = order.get("cancelledAt")
    cancelled_dt = _parse_iso(cancelled_raw) if cancelled_raw else None
    cancel_reason = order.get("cancelReason")
    cancel_reason_str = "" if cancel_reason is None else str(cancel_reason)

    refunds_payload = order.get("refunds")

    return {
        "shopify_gid": shopify_gid,
        "shop_name": shop_name,
        "order_id": merchant_order_number,
        "legacy_order_id": legacy_order_id,
        "customer_legacy_id": customer_legacy_id,
        "created_at": _format_ts(dt_utc),
        "beijing_created_at": _format_ts(beijing_created_at),
        "use_created_at": _format_ts(use_created_at),
        "berlin_created_at": _format_ts(berlin_created_at),
        "order_status": order_status,
        "product_quantity": product_quantity,
        "total_product_amount": total_product_amount,
        "shippingfee": shippingfee,
        "total_product_discount": total_product_discount,
        "discountfee": discountfee,
        "tax": tax,
        "logistics_status": logistics_status,
        "marketing_intention": marketing_intention,
        "currency_code": currency_code,
        "total_price": total_price,
        "product_sales": product_sales,
        "totalrefunded": totalrefunded,
        "discount_method": discount_method,
        "customer_display_name": customer_display_name or "",
        "customer_email": customer_email,
        "customer_phone": customer_phone,
        "bill_country": bill_country,
        "shipping_address1": shipping_address1,
        "shipping_address2": shipping_address2,
        "shipping_city": shipping_city,
        "shipping_province": shipping_province,
        "shipping_country": shipping_country,
        "shipping_zip": shipping_zip,
        "shipping_phone": shipping_phone,
        "shipping_name": ship_name,
        "discount_code": discount_code,
        "updated_at": _format_ts(updated_dt),
        "cancelled_at": _format_ts(cancelled_dt),
        "cancel_reason": cancel_reason_str,
        "product_details": json.dumps(product_details, ensure_ascii=False),
        "discount_information": json.dumps(discount_information, ensure_ascii=False),
        "billing_address": json.dumps(billing_dump, ensure_ascii=False),
        "refunds": json.dumps(refunds_payload, ensure_ascii=False),
    }


def row_dict_to_tuple(row: dict[str, Any], *, etl_synced_at: str) -> tuple[Any, ...]:
    """与 INSERT 列顺序一致（末尾追加同步时间）。"""

    return tuple(row[column] for column in SHOPIFY_ORDER_COLUMNS[:-1]) + (etl_synced_at,)
