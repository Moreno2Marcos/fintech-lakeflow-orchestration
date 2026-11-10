# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook Task — Remove Duplicate Installment Rows
# MAGIC
# MAGIC **Objective:** Remove identical full-row duplicates from the joined installment Delta table.
# MAGIC
# MAGIC ## Why?
# MAGIC
# MAGIC **For our project:** Retain one copy of each identical installment record before transformation.
# MAGIC
# MAGIC **For Data Engineering:** Execute corrective processing when the upstream If/else condition detects duplicates.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 01 — Configure Notebook Parameters
# MAGIC
# MAGIC Read the catalog and schema parameters to locate the joined installment table.

# COMMAND ----------

# Define defaults for manual execution.
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "fintech_jobs")

# Read the current parameter values.
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
namespace = f"{catalog}.{schema}"

# COMMAND ----------

# MAGIC %md
# MAGIC #### 02 — Remove Exact Duplicate Rows
# MAGIC
# MAGIC Read the joined installment Delta table, retain distinct full rows, and overwrite the same table. Compare the record counts before and after processing.

# COMMAND ----------

# Read the joined installment table.
target_table = f"{namespace}.customers_installments_silver"
source_df = spark.table(target_table)
before_count = source_df.count()

# Remove duplicates across all columns and persist the result.
distinct_df = source_df.dropDuplicates()

distinct_df.write.format("delta").mode("overwrite").saveAsTable(
    target_table
)

# Verify the persisted record count.
after_count = spark.table(target_table).count()

print(f"Rows before: {before_count}")
print(f"Rows after: {after_count}")
print(f"Duplicates removed: {before_count - after_count}")