# Databricks notebook source
# MAGIC %md
# MAGIC #### 01 — Validate Dynamic Workflow Outputs
# MAGIC
# MAGIC Inspect the curated installment schema, record count, and payment totals.

# COMMAND ----------

# Define defaults for manual notebook execution.
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "fintech_jobs")

# Read the existing notebook parameters.
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
namespace = f"{catalog}.{schema}"

# Inspect the persisted installment schema and record count.
curated_df = spark.table(f"{namespace}.customers_installments_curated")
curated_df.printSchema()
print(f"Curated installment records: {curated_df.count()}")

# Verify payment counts and amounts.
display(spark.sql(f"""
    SELECT payment_status,
           COUNT(*) AS installment_count,
           SUM(amount_due) AS total_amount_due
    FROM {namespace}.customers_installments_curated
    GROUP BY payment_status
    ORDER BY payment_status
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC Confirm that each state report contains exactly the matching
# MAGIC applications from the joined dataset.

# COMMAND ----------

from pyspark.sql import functions as F

applications_df = spark.table(
    f"{namespace}.customers_applications_silver"
)

validation_rows = []

for state in ["PE", "SP", "RJ", "MG", "BA"]:
    expected_df = applications_df.filter(F.col("state") == F.lit(state))

    # Align the original columns before comparing the records.
    actual_df = spark.table(
        f"{namespace}.customers_applications_{state.lower()}_silver"
    ).select(*applications_df.columns)

    assert actual_df.exceptAll(expected_df).count() == 0, (
        f"Unexpected or extra rows for {state}"
    )
    assert expected_df.exceptAll(actual_df).count() == 0, (
        f"Missing rows for {state}"
    )

    validation_rows.append((state, actual_df.count()))

display(spark.createDataFrame(
    validation_rows, ["state", "application_count"]
))
print("All five state reports match the source applications.")