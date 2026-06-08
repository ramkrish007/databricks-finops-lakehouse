# Databricks notebook source

from pyspark.sql.functions import (
    current_timestamp,
    current_date,
    col
)

# ==================================================
# Bronze Layer Ingestion
#
# Source:
# Unity Catalog Volume
#
# Target:
# finops.bronze.*
#
# Purpose:
# Ingest raw CSV and JSON files into Bronze Delta
# tables with standard audit columns.
# ==================================================


# --------------------------------------------------
# Common Bronze Audit Columns
# --------------------------------------------------

def add_audit_columns(df):

    return (
        df
        .withColumn(
            "ingestion_timestamp",
            current_timestamp()
        )
        .withColumn(
            "load_date",
            current_date()
        )
        .withColumn(
            "source_file_name",
            col("_metadata.file_name")
        )
    )


# --------------------------------------------------
# Metadata Driven Source Configuration
# --------------------------------------------------

sources = [

    {
        "source_name": "departments",
        "file_type": "csv",
        "path":
            "/Volumes/finops/bronze/raw_landing/departments.csv",
        "target_table":
            "finops.bronze.departments_raw"
    },

    {
        "source_name": "department_changes",
        "file_type": "csv",
        "path":
            "/Volumes/finops/bronze/raw_landing/department_changes.csv",
        "target_table":
            "finops.bronze.department_changes_raw"
    },

    {
        "source_name": "software_licenses",
        "file_type": "csv",
        "path":
            "/Volumes/finops/bronze/raw_landing/software_licenses.csv",
        "target_table":
            "finops.bronze.software_licenses_raw"
    },

    {
        "source_name": "cloud_usage",
        "file_type": "json",
        "path":
            "/Volumes/finops/bronze/raw_landing/cloud_usage_2026_*.json",
        "target_table":
            "finops.bronze.cloud_usage_raw"
    }
]


# --------------------------------------------------
# Bronze Ingestion Loop
# --------------------------------------------------

for source in sources:

    print(
        f"Loading source: "
        f"{source['source_name']}"
    )

    # --------------------------
    # Read Source
    # --------------------------

    if source["file_type"] == "csv":

        df = (
            spark.read
                 .option(
                     "header",
                     "true"
                 )
                 .csv(
                     source["path"]
                 )
        )

    elif source["file_type"] == "json":

        df = (
            spark.read
                 .json(
                     source["path"]
                 )
        )

    else:

        raise ValueError(
            f"Unsupported file type: "
            f"{source['file_type']}"
        )

    # --------------------------
    # Add Audit Columns
    # --------------------------

    df = add_audit_columns(df)

    # --------------------------
    # Write Bronze Table
    # --------------------------

    (
        df.write
          .format("delta")
          .mode("overwrite")
          .saveAsTable(
              source["target_table"]
          )
    )

    # --------------------------
    # Validation
    # --------------------------

    record_count = df.count()

    print(
        f"Loaded "
        f"{source['target_table']} "
        f"with "
        f"{record_count} records"
    )


print("Bronze ingestion completed successfully.")