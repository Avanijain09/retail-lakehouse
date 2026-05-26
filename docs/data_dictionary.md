# Data Dictionary

## orders

| Column | Type | Description |
|---|---|---|
| order_id | string | Unique order identifier (Primary Key) |
| customer_id | string | Customer who placed the order |
| order_status | string | Current order status |
| order_purchase_timestamp | timestamp | Order purchase datetime |
| order_approved_at | timestamp | Order approval datetime |
| order_delivered_carrier_date | timestamp | Carrier pickup datetime |
| order_delivered_customer_date | timestamp | Customer delivery datetime |
| order_estimated_delivery_date | timestamp | Expected delivery date |

---

## order_items

| Column | Type | Description |
|---|---|---|
| order_id | string | Order identifier (Foreign Key) |
| order_item_id | integer | Item sequence inside order |
| product_id | string | Product identifier |
| seller_id | string | Seller/store identifier |
| shipping_limit_date | timestamp | Shipping deadline |
| price | decimal | Product selling price |
| freight_value | decimal | Shipping charge |

**Grain:**
1 row = 1 product inside 1 order

---

## customers

| Column | Type | Description |
|---|---|---|
| customer_id | string | Customer session/order identifier |
| customer_unique_id | string | Real unique customer identifier |
| customer_zip_code_prefix | integer | ZIP code prefix |
| customer_city | string | Customer city |
| customer_state | string | Customer state |

---

## products

| Column | Type | Description |
|---|---|---|
| product_id | string | Product identifier |
| product_category_name | string | Product category |
| product_name_lenght | integer | Product name length |
| product_description_lenght | integer | Product description length |
| product_photos_qty | integer | Number of product photos |
| product_weight_g | decimal | Product weight in grams |
| product_length_cm | decimal | Product length in cm |
| product_height_cm | decimal | Product height in cm |
| product_width_cm | decimal | Product width in cm |

---

## stores

| Column | Type | Description |
|---|---|---|
| seller_id | string | Seller/store identifier |
| seller_zip_code_prefix | integer | ZIP code prefix |
| seller_city | string | Seller city |
| seller_state | string | Seller state |