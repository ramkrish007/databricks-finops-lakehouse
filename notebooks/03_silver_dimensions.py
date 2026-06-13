# Databricks notebook source

from pyspark.sql.functions import (
    col,
    to_date,
    expr,
    row_number,
    explode
)

from pyspark.sql.window import Window

# ==================================================
# Read Bronze Cloud Usage
# ==================================================

cloud_df = spark.table(
    "finops.bronze.cloud_usage_raw"
)

# ==================================================
# Data Type Conversion
# ==================================================

fact_cloud_usage_df = (

    cloud_df

    .withColumn(
        "monthly_cost",
        expr(
            "try_cast(monthly_cost as decimal(18,2))"
        )
    )

    .withColumn(
        "cpu_utilization",
        expr(
            "try_cast(cpu_utilization as int)"
        )
    )

    .withColumn(
        "memory_utilization",
        expr(
            "try_cast(memory_utilization as int)"
        )
    )

    .withColumn(
        "usage_date",
        to_date("usage_date")
    )

)

# ==================================================
# Deduplicate Cloud Resources
# ==================================================

fact_dedup_window = (

    Window

    .partitionBy(
        "resource_id"
    )

    .orderBy(
        col("ingestion_timestamp").desc()
    )

)

fact_cloud_usage_dedup_df = (

    fact_cloud_usage_df

    .withColumn(
        "rn",
        row_number().over(
            fact_dedup_window
        )
    )

    .filter(
        col("rn") == 1
    )

    .drop("rn")

)

# ==================================================
# Quarantine Invalid Records
# ==================================================

quarantine_cloud_usage_df = (

    fact_cloud_usage_dedup_df

    .filter(

        col("department_id").isNull()

        |

        col("monthly_cost").isNull()

        |

        col("cpu_utilization").isNull()

        |

        col("memory_utilization").isNull()

        |

        (col("monthly_cost") < 0)

        |

        (~col("cpu_utilization").between(0,100))

        |

        (~col("memory_utilization").between(0,100))

    )

)

# ==================================================
# Valid Records
# ==================================================

valid_cloud_usage_df = (

    fact_cloud_usage_dedup_df

    .filter(
        col("department_id").isNotNull()
    )

    .filter(
        col("monthly_cost").isNotNull()
    )

    .filter(
        col("cpu_utilization").isNotNull()
    )

    .filter(
        col("memory_utilization").isNotNull()
    )

    .filter(
        col("monthly_cost") >= 0
    )

    .filter(
        col("cpu_utilization").between(0,100)
    )

    .filter(
        col("memory_utilization").between(0,100)
    )

)

# ==================================================
# Write Silver Fact Table
# ==================================================

(
    valid_cloud_usage_df.write
        .format("delta")
        .option(
            "mergeSchema",
            "true"
        )
        .mode("append")
        .saveAsTable(
            "finops.silver.fact_cloud_usage_stg"
        )
)

spark.sql(
"""
MERGE INTO finops.silver.fact_cloud_usage AS target
USING finops.silver.fact_cloud_usage_stg AS source

ON target.resource_id = source.resource_id

WHEN MATCHED THEN
UPDATE SET
    target.department_id      = source.department_id,
    target.service            = source.service,
    target.region             = source.region,
    target.environment        = source.environment,
    target.instance_type      = source.instance_type,
    target.monthly_cost       = source.monthly_cost,
    target.cpu_utilization    = source.cpu_utilization,
    target.memory_utilization = source.memory_utilization,
    target.migration_wave     = source.migration_wave,
    target.project_name       = source.project_name,
    target.usage_date         = source.usage_date,
    target.cost_center        = source.cost_center

WHEN NOT MATCHED THEN
INSERT (
    resource_id,
    department_id,
    service,
    region,
    environment,
    instance_type,
    monthly_cost,
    cpu_utilization,
    memory_utilization,
    migration_wave,
    project_name,
    usage_date,
    ingestion_timestamp,
    load_date,
    source_file_name,
    cost_center
)
VALUES (
    source.resource_id,
    source.department_id,
    source.service,
    source.region,
    source.environment,
    source.instance_type,
    source.monthly_cost,
    source.cpu_utilization,
    source.memory_utilization,
    source.migration_wave,
    source.project_name,
    source.usage_date,
    source.ingestion_timestamp,
    source.load_date,
    source.source_file_name,
    source.cost_center
)
"""
)

spark.sql(
"""
TRUNCATE TABLE
finops.silver.fact_cloud_usage_stg
"""
)

# ==================================================
# Write Quarantine Table
# ==================================================

(
    quarantine_cloud_usage_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(
            "finops.silver.quarantine_cloud_usage"
        )
)

# ==================================================
# Validation Metrics
# ==================================================

print(
    "Valid Records:",
    valid_cloud_usage_df.count()
)

print(
    "Quarantine Records:",
    quarantine_cloud_usage_df.count()
)

# ==================================================
# Build Cloud Usage Tags
# ==================================================

cloud_usage_tags_df = (

    fact_cloud_usage_dedup_df

    .select(
        "resource_id",
        "tags",
        "ingestion_timestamp",
        "load_date",
        "source_file_name"
    )

)

# ==================================================
# Explode Tags Array
# ==================================================

cloud_usage_tags_df = (

    cloud_usage_tags_df

    .withColumn(
        "tag",
        explode("tags")
    )

)

# ==================================================
# Flatten Tag Structure
# ==================================================

cloud_usage_tags_df = (

    cloud_usage_tags_df

    .select(
        "resource_id",

        col("tag.key")
            .alias("tag_key"),

        col("tag.value")
            .alias("tag_value"),

        "ingestion_timestamp",
        "load_date",
        "source_file_name"
    )

)

# ==================================================
# Write Cloud Usage Tags Table
# ==================================================

(
    cloud_usage_tags_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(
            "finops.silver.cloud_usage_tags"
        )
)

# ==================================================
# Validation
# ==================================================

print(
    "Cloud Usage Tags:",
    cloud_usage_tags_df.count()
)

# ==================================================
# Broadcast Join Demonstration
# ==================================================

from pyspark.sql.functions import broadcast

fact_df = spark.table(
    "finops.silver.fact_cloud_usage"
)

department_df = spark.table(
    "finops.silver.dim_department"
)

fact_department_df = (

    fact_df.alias("f")

    .join(

        broadcast(
            department_df.alias("d")
        ),

        on="department_id",

        how="left"

    )

)

# ==================================================
# Validation
# ==================================================

print(
    "Joined Records:",
    fact_department_df.count()
)

fact_department_df.explain(True)