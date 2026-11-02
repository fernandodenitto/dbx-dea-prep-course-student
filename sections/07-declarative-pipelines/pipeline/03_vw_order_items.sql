-- Lakeshore SDP pipeline · step 3 — intermediate logic
-- A TEMPORARY VIEW: pipeline-private, never published to the catalog.
-- The explode of lesson 6.4, reused by downstream gold.

CREATE TEMPORARY VIEW vw_order_items AS
SELECT
  order_id,
  order_ts,
  item.product_id,
  item.quantity,
  item.unit_price,
  item.quantity * item.unit_price AS line_total
FROM (
  SELECT order_id, order_ts, explode(items) AS item
  FROM lakeshore.silver.sdp_orders
);
