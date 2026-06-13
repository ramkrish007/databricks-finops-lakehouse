dbutils.notebook.run(
    "01_bronze_ingestion",
    0
)

dbutils.notebook.run(
    "03_silver_dimensions",
    0
)

dbutils.notebook.run(
    "03_gold_department_spend",
    0
)

dbutils.notebook.run(
    "04_gold_underutilized_resources",
    0
)

dbutils.notebook.run(
    "05_gold_service_environment",
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