# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook Task — Credit Applications by State
# MAGIC
# MAGIC **Objective:** Create an application report for the state supplied as a notebook parameter.
# MAGIC
# MAGIC ## Why?
# MAGIC
# MAGIC **For our project:** Produce state-specific credit application details from one joined dataset.
# MAGIC
# MAGIC **For Data Engineering:** Reuse one notebook across For each iterations by passing the current state as a parameter.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 01 — Configure Notebook Parameters
# MAGIC
# MAGIC Read the catalog, schema, and state parameters. Use `PE` as the default state for the initial manual test.

# COMMAND ----------

# Define defaults for manual execution.
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "fintech_jobs")
dbutils.widgets.text("state", "PE")

# Read the current parameter values.
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
namespace = f"{catalog}.{schema}"

state = dbutils.widgets.get("state").strip().upper()
assert state in {"PE", "SP", "RJ", "MG", "BA"}, f"Unexpected state: {state}"

# COMMAND ----------

# MAGIC %md
# MAGIC #### 02 — Build the State Application Report
# MAGIC
# MAGIC Filter the joined applications by the supplied state. Add application calendar fields and an approval indicator, then save a separate Delta table for that state.

# COMMAND ----------

from pyspark.sql import functions as F

# Filter applications and add reporting fields.
report_df = (
    spark.table(f"{namespace}.customers_applications_silver")
    .filter(F.col("state") == F.lit(state))
    .withColumn("application_year", F.year("application_date"))
    .withColumn("application_month", F.date_format("application_date", "MM"))
    .withColumn("is_approved", F.col("status") == F.lit("APPROVED"))
)

# Persist a separate report for the supplied state.
target_table = (
    f"{namespace}.customers_applications_{state.lower()}_silver"
)

report_df.write.format("delta").mode("overwrite").saveAsTable(
    target_table
)

# Inspect the persisted result.
stored_df = spark.table(target_table)

print(f"State: {state}")
print(f"Output table: {target_table}")
print(f"Application records: {stored_df.count()}")

display(stored_df.orderBy("application_id"))