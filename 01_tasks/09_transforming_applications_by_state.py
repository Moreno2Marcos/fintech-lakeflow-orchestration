# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Credit Applications by State
# MAGIC
# MAGIC **Objective:** Combine the five state reports, standardize customer
# MAGIC and application fields, and persist application detail and state
# MAGIC summary tables.
# MAGIC
# MAGIC ## Why?
# MAGIC
# MAGIC **For our project:** Prepare consistent application data for the
# MAGIC fintech operations dashboard.
# MAGIC
# MAGIC **For Data Engineering:** Practice shared transformation logic,
# MAGIC task dependencies, repeatable Delta writes, and recovery from
# MAGIC a controlled missing-column error.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 01 — Configure Notebook Parameters
# MAGIC
# MAGIC Read the catalog and schema parameters to locate the state reports
# MAGIC and define the output tables.

# COMMAND ----------

# Define defaults for manual execution.
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "fintech_jobs")

# Read the current parameter values.
catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
namespace = f"{catalog}.{schema}"

# COMMAND ----------

# MAGIC %md
# MAGIC #### 02 — Combine and Transform State Reports
# MAGIC
# MAGIC Standardize shared fields, combine the five state reports,
# MAGIC and prepare application detail and state summary tables.
# MAGIC
# MAGIC For the initial failure test, deliberately reference the missing
# MAGIC column `customer`. The existing source column is `customer_name`.
# MAGIC We will diagnose and correct this reference during the repair.

# COMMAND ----------

from functools import reduce
from pyspark.sql import functions as F

states = ["PE", "SP", "RJ", "MG", "BA"]


def clean_common(df):
    # Standardize customer names.
    df = df.withColumn(
        "customer_name",
        F.initcap(F.trim(F.col("customer_name")))
    )
    df = df.withColumn(
        "state",
        F.upper(F.trim(F.col("state")))
    )
    df = df.withColumn(
        "status",
        F.upper(F.trim(F.col("status")))
    )
    return df


# Read and standardize each state report.
state_frames = [
    clean_common(
        spark.table(
            f"{namespace}.customers_applications_{state.lower()}_silver"
        )
    )
    for state in states
]

# Combine reports by matching column names.
gold_df = reduce(
    lambda left, right: left.unionByName(right),
    state_frames
)

# Add reporting fields.
gold_df = (
    gold_df
    .withColumn("application_year", F.year("application_date"))
    .withColumn(
        "application_month",
        F.date_format("application_date", "MM")
    )
    .withColumn(
        "is_approved",
        F.col("status") == F.lit("APPROVED")
    )
)

detail_table = f"{namespace}.customers_applications_gold"
summary_table = (
    f"{namespace}.customers_applications_state_summary_gold"
)

# Persist application details.
gold_df.write.format("delta").mode("overwrite").saveAsTable(
    detail_table
)

# Aggregate application metrics by state.
summary_df = gold_df.groupBy("state").agg(
    F.count("*").alias("application_count"),
    F.countDistinct("customer_id").alias("customer_count"),
    F.sum("requested_amount").alias("total_requested_amount"),
    F.sum(
        F.col("is_approved").cast("int")
    ).alias("approved_application_count"),
)

# Persist the state summary.
summary_df.write.format("delta").mode("overwrite").saveAsTable(
    summary_table
)

# Inspect persisted outputs after successful execution.
stored_detail = spark.table(detail_table)
stored_summary = spark.table(summary_table)

print(f"Application detail records: {stored_detail.count()}")
print(f"State summary records: {stored_summary.count()}")

display(stored_summary.orderBy("state"))