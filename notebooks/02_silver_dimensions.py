# Databricks notebook source

from pyspark.sql.functions import (
    col,
    to_date,
    lead,
    row_number,
    when,
    lit,
    date_sub
)

from pyspark.sql.window import Window
from pyspark.sql.types import (
    DecimalType,
    DateType
)

# ==================================================
# Build dim_department
# ==================================================

departments_df = spark.table(
    "finops.bronze.departments_raw"
)

dim_department_df = (

    departments_df

    .withColumn(
        "monthly_budget",
        col("monthly_budget")
            .cast(DecimalType(18,2))
    )

    .withColumn(
        "created_date",
        to_date("created_date")
    )

)

(
    dim_department_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(
            "finops.silver.dim_department"
        )
)

# ==================================================
# Read Department Changes
# ==================================================

changes_df = (
    spark.table(
        "finops.bronze.department_changes_raw"
    )
    .withColumn(
        "effective_date",
        to_date("effective_date")
    )
    .withColumn(
        "monthly_budget",
        col("monthly_budget")
            .cast(DecimalType(18,2))
    )
)

# ==================================================
# Deduplicate Same-Day Changes
# ==================================================

dedup_window = (
    Window
    .partitionBy(
        "department_id",
        "effective_date"
    )
    .orderBy(
        col("ingestion_timestamp").desc()
    )
)

changes_dedup_df = (
    changes_df
    .withColumn(
        "rn",
        row_number().over(dedup_window)
    )
    .filter(
        col("rn") == 1
    )
    .drop("rn")
)

# ==================================================
# Build SCD Timeline
# ==================================================

scd_window = (
    Window
    .partitionBy("department_id")
    .orderBy("effective_date")
)

changes_scd_df = (
    changes_dedup_df
    .withColumn(
        "next_effective_date",
        lead("effective_date")
        .over(scd_window)
    )
)

# ==================================================
# Build SCD Type 2 Columns
# ==================================================

dim_department_scd_df = (

    changes_scd_df

    .withColumn(
        "effective_start_date",
        col("effective_date")
    )

    .withColumn(
        "effective_end_date",
        when(
            col("next_effective_date").isNull(),
            lit("9999-12-31").cast(DateType())
        )
        .otherwise(
            date_sub(
                col("next_effective_date"),
                1
            )
        )
    )

    .withColumn(
        "is_current",
        when(
            col("next_effective_date").isNull(),
            lit("Y")
        )
        .otherwise(
            lit("N")
        )
    )
)

# ==================================================
# Final SCD Output
# ==================================================

final_scd_df = (

    dim_department_scd_df

    .select(
        "department_id",
        "department_name",
        "manager_name",
        "monthly_budget",
        "effective_start_date",
        "effective_end_date",
        "is_current",
        "ingestion_timestamp",
        "load_date",
        "source_file_name"
    )
)

(
    final_scd_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(
            "finops.silver.dim_department_scd"
        )
)