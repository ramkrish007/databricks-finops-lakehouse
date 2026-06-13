dbutils.notebook.run(
    "01_bronze_ingestion",
    0
)

dbutils.notebook.run(
    "02_silver_dimensions",
    0
)

dbutils.notebook.run(
    "03_silver_dimensions",
    0
)

dbutils.notebook.run(
    "04_gold_layer",
    0
)

spark.sql("""
OPTIMIZE finops.silver.fact_cloud_usage
""")

spark.sql("""
OPTIMIZE finops.silver.fact_cloud_usage
ZORDER BY (
    department_id,
    usage_date
)
""")