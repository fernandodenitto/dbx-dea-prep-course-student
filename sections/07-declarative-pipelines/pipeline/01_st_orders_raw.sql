-- Lakeshore SDP pipeline · step 1 — BRONZE
-- Declarative Auto Loader ingestion: everything lesson 5.5 built by hand,
-- as one statement. Note STREAM read_files (streaming), not read_files (batch).

CREATE OR REFRESH STREAMING TABLE lakeshore.bronze.sdp_orders_raw
COMMENT 'Raw orders ingested declaratively from the landing volume (SDP demo flow)'
AS SELECT
  *,
  _metadata.file_path AS source_file
FROM STREAM read_files(
  '/Volumes/lakeshore/landing/files/orders',
  format => 'json'
);
