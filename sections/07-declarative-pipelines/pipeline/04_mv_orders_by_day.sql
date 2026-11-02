-- Lakeshore SDP pipeline · step 4 — GOLD
-- A MATERIALIZED VIEW as one node of the graph. Batch read of the temp view
-- (no STREAM), because an aggregation has to recompute to stay correct.

CREATE OR REFRESH MATERIALIZED VIEW lakeshore.gold.sdp_orders_by_day
COMMENT 'Daily order revenue, declaratively maintained (SDP demo flow)'
AS SELECT
  date(order_ts)             AS order_date,
  count(DISTINCT order_id)   AS n_orders,
  round(sum(line_total), 2)  AS revenue
FROM vw_order_items
GROUP BY date(order_ts);
