# Source of the bundle's pipeline: one materialized view with one row.
# Section 7 wrote materialized views in SQL; a pipeline takes Python as well.

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(comment="One row: days covered and total revenue")
def revenue_snapshot():
    source = spark.conf.get("source_table")
    return spark.read.table(source).agg(
        F.count("*").alias("days"),
        F.round(F.sum("revenue"), 2).alias("total_revenue"),
    )
