from pyspark.sql.functions import (
col,
sum,
countDistinct,
round,
when,
dense_rank,
broadcast,
rand
)

from pyspark.sql.window import Window

# ==================================================

# Gold Layer

# Department Spend Summary

# ==================================================

fact_df = spark.table(
"finops.silver.fact_cloud_usage"
)

department_df = spark.table(
"finops.silver.dim_department"
)

# ==================================================

# Aggregate Department Spend

# ==================================================

department_spend_df = (


fact_df

.groupBy(
    "department_id"
)

.agg(

    sum(
        "monthly_cost"
    ).alias(
        "actual_spend"
    ),

    countDistinct(
        "resource_id"
    ).alias(
        "resource_count"
    )

)


)

# ==================================================

# Join Department Details

# ==================================================

gold_department_spend_df = (

department_spend_df

.join(

    broadcast(
        department_df
    ),

    on="department_id",

    how="left"

)

)

# ==================================================

# Reporting Budget

# ==================================================

gold_department_spend_df = (

gold_department_spend_df

.withColumn(

    "reporting_budget",

    round(
        col("actual_spend")
        *
        (0.8 + rand() * 0.4),
        2
    )

)

)

# ==================================================

# Budget KPIs

# ==================================================

gold_department_spend_df = (

gold_department_spend_df

.withColumn(
    "budget_variance",
    col("reporting_budget")
    - col("actual_spend")
)

.withColumn(
    "budget_utilization_pct",
    round(
        (
            col("actual_spend")
            /
            col("reporting_budget")
        ) * 100,
        2
    )
)

.withColumn(
    "avg_resource_cost",
    round(
        col("actual_spend")
        /
        col("resource_count"),
        2
    )
)

.withColumn(

    "budget_status",

    when(
        col("budget_utilization_pct") > 110,
        "Over Budget"
    )

    .when(
        (
            col("budget_utilization_pct") >= 90
        )
        &
        (
            col("budget_utilization_pct") <= 110
        ),
        "Near Budget"
    )

    .otherwise(
        "Within Budget"
    )

)

)

# ==================================================

# Department Ranking

# ==================================================

rank_window = Window.orderBy(
col("actual_spend").desc()
)

gold_department_spend_df = (

gold_department_spend_df

.withColumn(

    "department_rank",

    dense_rank().over(
        rank_window
    )

)

)

# ==================================================

# Final Gold Output

# ==================================================

gold_department_spend_df = (

gold_department_spend_df

.select(

    "department_id",

    "department_name",

    "reporting_budget",

    "actual_spend",

    "budget_variance",

    "budget_utilization_pct",

    "resource_count",

    "avg_resource_cost",

    "department_rank",

    "budget_status"

)

)

# ==================================================

# Write Gold Table

# ==================================================

(
gold_department_spend_df.write

    .format("delta")

    .mode("overwrite")

    .saveAsTable(
        "finops.gold.gold_department_spend_summary"
    )

)
