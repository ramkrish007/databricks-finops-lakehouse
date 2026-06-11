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

# ==================================================
# Gold Layer
# Underutilized Resources Summary
# ==================================================

fact_df = spark.table(
    "finops.silver.fact_cloud_usage"
)

department_df = spark.table(
    "finops.silver.dim_department"
)

# ==================================================
# Join Department Details
# ==================================================

gold_underutilized_df = (

    fact_df

    .join(

        broadcast(

            department_df.select(
                "department_id",
                "department_name"
            )

        ),

        on="department_id",

        how="left"

    )

)

# ==================================================
# Resource Health Classification
# ==================================================

gold_underutilized_df = (

    gold_underutilized_df

    .withColumn(

        "resource_health",

        when(

            (
                col("cpu_utilization") < 20
            )

            |

            (
                col("memory_utilization") < 20
            ),

            "Underutilized"

        )

        .when(

            (
                (
                    col("cpu_utilization") >= 20
                )
                &
                (
                    col("cpu_utilization") <= 50
                )
            )

            |

            (
                (
                    col("memory_utilization") >= 20
                )
                &
                (
                    col("memory_utilization") <= 50
                )
            ),

            "Moderate"

        )

        .otherwise(
            "Healthy"
        )

    )

)

# ==================================================
# Potential Savings
# ==================================================

gold_underutilized_df = (

    gold_underutilized_df

    .withColumn(

        "potential_monthly_savings",

        when(

            col("resource_health")
            == "Underutilized",

            col("monthly_cost")

        )

        .when(

            col("resource_health")
            == "Moderate",

            round(
                col("monthly_cost") * 0.25,
                2
            )

        )

        .otherwise(
            0
        )

    )

)

# ==================================================
# Annual Savings
# ==================================================

gold_underutilized_df = (

    gold_underutilized_df

    .withColumn(

        "potential_annual_savings",

        round(

            col(
                "potential_monthly_savings"
            ) * 12,

            2

        )

    )

)

# ==================================================
# Final Gold Output
# ==================================================

gold_underutilized_df = (

    gold_underutilized_df

    .select(

        "resource_id",

        "department_id",

        "department_name",

        "service",

        "environment",

        "monthly_cost",

        "cpu_utilization",

        "memory_utilization",

        "resource_health",

        "potential_monthly_savings",

        "potential_annual_savings"

    )

)

# ==================================================
# Write Gold Table
# ==================================================

(
    gold_underutilized_df.write

        .format("delta")

        .mode("overwrite")

        .saveAsTable(
            "finops.gold.gold_underutilized_resources"
        )
)

# ==================================================

# Gold Layer

# Service Cost Summary

# ==================================================

fact_df = spark.table(
"finops.silver.fact_cloud_usage"
)

service_summary_df = (

fact_df

.groupBy(
    "service"
)

.agg(

    sum(
        "monthly_cost"
    ).alias(
        "total_spend"
    ),

    countDistinct(
        "resource_id"
    ).alias(
        "resource_count"
    )

)

)

# ==================================================

# Average Resource Cost

# ==================================================

service_summary_df = (

service_summary_df

.withColumn(

    "avg_resource_cost",

    round(

        col("total_spend")
        /
        col("resource_count"),

        2

    )

)

)

# ==================================================

# Spend Percentage

# ==================================================

total_spend = (

service_summary_df

.agg(
    sum(
        "total_spend"
    )
)

.collect()[0][0]

)

service_summary_df = (

service_summary_df

.withColumn(

    "spend_percentage",

    round(

        (
            col("total_spend")
            /
            total_spend
        ) * 100,

        2

    )

)

)

# ==================================================

# Service Ranking

# ==================================================

rank_window = Window.orderBy(
col("total_spend").desc()
)

service_summary_df = (

service_summary_df

.withColumn(

    "service_rank",

    dense_rank().over(
        rank_window
    )

)

)

# ==================================================

# Final Gold Output

# ==================================================

service_summary_df = (

service_summary_df

.select(

    "service",

    "total_spend",

    "resource_count",

    "avg_resource_cost",

    "spend_percentage",

    "service_rank"

)

)

# ==================================================

# Write Gold Table

# ==================================================

(
service_summary_df.write

    .format("delta")

    .mode("overwrite")

    .saveAsTable(
        "finops.gold.gold_service_cost_summary"
    )

)

# ==================================================

# Gold Layer

# Environment Cost Summary

# ==================================================

fact_df = spark.table(
"finops.silver.fact_cloud_usage"
)

environment_summary_df = (

fact_df

.groupBy(
    "environment"
)

.agg(

    sum(
        "monthly_cost"
    ).alias(
        "total_spend"
    ),

    countDistinct(
        "resource_id"
    ).alias(
        "resource_count"
    )

)

)

# ==================================================

# Average Resource Cost

# ==================================================

environment_summary_df = (

environment_summary_df

.withColumn(

    "avg_resource_cost",

    round(

        col("total_spend")
        /
        col("resource_count"),

        2

    )

)

)

# ==================================================

# Spend Percentage

# ==================================================

total_spend = (

environment_summary_df

.agg(
    sum(
        "total_spend"
    )
)

.collect()[0][0]

)

environment_summary_df = (

environment_summary_df

.withColumn(

    "spend_percentage",

    round(

        (
            col("total_spend")
            /
            total_spend
        ) * 100,

        2

    )

)

)

# ==================================================

# Environment Ranking

# ==================================================

rank_window = Window.orderBy(
col("total_spend").desc()
)

environment_summary_df = (

environment_summary_df

.withColumn(

    "environment_rank",

    dense_rank().over(
        rank_window
    )

)

)

# ==================================================

# Final Gold Output

# ==================================================

environment_summary_df = (

environment_summary_df

.select(

    "environment",

    "total_spend",

    "resource_count",

    "avg_resource_cost",

    "spend_percentage",

    "environment_rank"

)

)

# ==================================================

# Write Gold Table

# ==================================================

(
environment_summary_df.write

    .format("delta")

    .mode("overwrite")

    .saveAsTable(
        "finops.gold.gold_environment_cost_summary"
    )

)

