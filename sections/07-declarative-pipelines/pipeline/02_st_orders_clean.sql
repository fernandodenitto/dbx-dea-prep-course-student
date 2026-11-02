-- Lakeshore SDP pipeline · step 2 — SILVER
-- Cleaning + EXPECTATIONS: the quality battery of lesson 6.6, declared on the
-- table itself. Three policies on show: drop, drop, warn.

CREATE OR REFRESH STREAMING TABLE lakeshore.silver.sdp_orders (
  CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL)    ON VIOLATION DROP ROW,
  CONSTRAINT valid_customer EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT sane_timestamp EXPECT (order_ts <= current_timestamp())
)
COMMENT 'Cleaned orders with declared quality expectations (SDP demo flow)'
AS SELECT
  order_id,
  customer_id,
  CAST(order_ts AS TIMESTAMP) AS order_ts,
  -- No from_json here: read_files infers TYPED columns by default, the opposite of
  -- raw cloudFiles (lesson 5.5), so items is already ARRAY<STRUCT<...>>.
  items,
  sales_channel,
  source_file
FROM STREAM(lakeshore.bronze.sdp_orders_raw);
