# Databricks notebook source
# MAGIC %md
# MAGIC # Validate Recovery and Monitoring Outputs
# MAGIC
# MAGIC **Objective:** Validate application details, state summaries,
# MAGIC and installment totals after the selective repair.
# MAGIC
# MAGIC ## Why?
# MAGIC
# MAGIC **For our project:** Confirm that the repaired outputs contain
# MAGIC the expected records and financial totals before dashboard creation.
# MAGIC
# MAGIC **For Data Engineering:** Verify source-to-output consistency,
# MAGIC identifier uniqueness, and aggregate accuracy after recovery.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 01 — Configure Validation Parameters
# MAGIC
# MAGIC Read the catalog and schema parameters to locate the source
# MAGIC and repaired output tables.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "fintech_jobs")

catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
namespace = f"{catalog}.{schema}"

# COMMAND ----------

# MAGIC %md
# MAGIC #### 02 — Validate Application Details and State Summaries
# MAGIC
# MAGIC Compare the repaired application details with the source records
# MAGIC after applying the expected standardization.
# MAGIC
# MAGIC Verify application identifiers, requested amounts, approval counts,
# MAGIC and the persisted state summary.

# COMMAND ----------

from decimal import Decimal
from pyspark.sql import functions as F

# Read the source and repaired tables.
source_df = spark.table(
    f"{namespace}.customers_applications_silver"
)
gold_df = spark.table(
    f"{namespace}.customers_applications_gold"
)
summary_df = spark.table(
    f"{namespace}.customers_applications_state_summary_gold"
)

# Define the expected application records from the source.
expected_df = (
    source_df
    .withColumn(
        "customer_name",
        F.initcap(F.trim(F.col("customer_name")))
    )
    .withColumn("state", F.upper(F.trim(F.col("state"))))
    .withColumn("status", F.upper(F.trim(F.col("status"))))
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

# Align columns and compare records in both directions.
actual_df = gold_df.select(*expected_df.columns)

assert actual_df.exceptAll(expected_df).count() == 0, (
    "Unexpected application records"
)
assert expected_df.exceptAll(actual_df).count() == 0, (
    "Missing application records"
)

# Verify record counts and identifier uniqueness.
application_count = gold_df.count()
state_summary_count = summary_df.count()

assert application_count == 6, "Expected six applications"
assert gold_df.filter(
    F.col("application_id").isNull()
).count() == 0, "Missing application identifiers"
assert gold_df.select(
    "application_id"
).distinct().count() == 6, "Duplicate application identifiers"
assert state_summary_count == 5, "Expected five state summaries"

# Verify financial and approval totals.
total_amount = gold_df.agg(
    F.sum("requested_amount")
).first()[0]

assert total_amount == Decimal("35800.00"), (
    "Unexpected total requested amount"
)
assert gold_df.filter(
    F.col("is_approved")
).count() == 3, "Expected three approved applications"

# Calculate expected state metrics from the source records.
expected_summary = expected_df.groupBy("state").agg(
    F.count("*").alias("application_count"),
    F.countDistinct("customer_id").alias("customer_count"),
    F.sum("requested_amount").alias("total_requested_amount"),
    F.sum(
        F.col("is_approved").cast("int")
    ).alias("approved_application_count"),
)

# Compare the persisted summary with the expected metrics.
actual_summary = summary_df.select(*expected_summary.columns)

assert actual_summary.exceptAll(expected_summary).count() == 0, (
    "Unexpected state-summary records"
)
assert expected_summary.exceptAll(actual_summary).count() == 0, (
    "Missing state-summary records"
)

# Show the validated outputs.
gold_df.printSchema()
display(summary_df.orderBy("state"))

print(f"Application detail records: {application_count}")
print(f"State summary records: {state_summary_count}")
print("Application detail and state summary validation passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 03 — Reconcile Installment Totals
# MAGIC
# MAGIC Verify that the curated table contains six unique installments.
# MAGIC Reconcile installment counts and amounts for PAID and PENDING.

# COMMAND ----------

from decimal import Decimal
from pyspark.sql import functions as F

installments_df = spark.table(
    f"{namespace}.customers_installments_curated"
)

# Verify record count and identifier uniqueness.
assert installments_df.count() == 6, (
    "Expected six curated installments"
)
assert installments_df.filter(
    F.col("installment_id").isNull()
).count() == 0, "Missing installment identifiers"
assert installments_df.select(
    "installment_id"
).distinct().count() == 6, "Duplicate installment identifiers"

# Aggregate payment counts and amounts.
payment_df = installments_df.groupBy("payment_status").agg(
    F.count("*").alias("installment_count"),
    F.sum("amount_due").alias("total_amount_due"),
)

actual = {
    row["payment_status"]: (
        row["installment_count"],
        row["total_amount_due"]
    )
    for row in payment_df.collect()
}

expected = {
    "PAID": (2, Decimal("5750.00")),
    "PENDING": (4, Decimal("12350.00")),
}

assert actual == expected, (
    "Installment payment totals do not reconcile"
)

display(payment_df.orderBy("payment_status"))
print("Installment reconciliation passed.")