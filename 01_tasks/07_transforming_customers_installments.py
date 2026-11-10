# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook Task — Curate Customer Installments
# MAGIC
# MAGIC **Objective:** Standardize customer details, convert installment fields to useful data types, and add due-date and payment indicators.
# MAGIC
# MAGIC ## Why?
# MAGIC
# MAGIC **For our project:** Produce typed installment details for queries and reporting.
# MAGIC
# MAGIC **For Data Engineering:** Apply consistent data types and run a shared transformation after the selected conditional path succeeds.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 01 — Configure Notebook Parameters
# MAGIC
# MAGIC Read the catalog and schema parameters to locate the source and output tables.

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
# MAGIC #### 02 — Standardize and Enrich Installment Details
# MAGIC
# MAGIC Convert identifiers, dates, and amounts to appropriate types and standardize text.
# MAGIC
# MAGIC Add `due_year`, `due_month`, and `is_paid`. Validate required converted values before writing the curated Delta table.

# COMMAND ----------

from pyspark.sql import functions as F

# Read the joined installment records.
source_df = spark.table(
    f"{namespace}.customers_installments_silver"
)

# Convert data types and standardize text.
typed_df = source_df.select(
    F.expr("try_cast(installment_id AS BIGINT)").alias("installment_id"),
    F.expr("try_cast(application_id AS BIGINT)").alias("application_id"),
    F.expr("try_cast(customer_id AS BIGINT)").alias("customer_id"),
    F.initcap(F.trim(F.col("customer_name"))).alias("customer_name"),
    F.upper(F.trim(F.col("state"))).alias("state"),
    F.expr("try_cast(due_date AS DATE)").alias("due_date"),
    F.expr("try_cast(amount_due AS DECIMAL(12,2))").alias("amount_due"),
    F.upper(F.trim(F.col("payment_status"))).alias("payment_status"),
)

# Stop processing if required converted values are missing.
invalid_rows = typed_df.filter(
    F.col("installment_id").isNull()
    | F.col("application_id").isNull()
    | F.col("customer_id").isNull()
    | F.col("due_date").isNull()
    | F.col("amount_due").isNull()
).count()

print(f"Invalid required values: {invalid_rows}")
assert invalid_rows == 0, "Inspect required values before continuing."

# Add calendar fields and a payment indicator.
curated_df = (
    typed_df
    .withColumn("due_year", F.year("due_date"))
    .withColumn("due_month", F.date_format("due_date", "MM"))
    .withColumn("is_paid", F.col("payment_status") == F.lit("PAID"))
)

# Persist the curated records.
target_table = f"{namespace}.customers_installments_curated"

curated_df.write.format("delta").mode("overwrite").saveAsTable(
    target_table
)

# Inspect the persisted result.
result_df = spark.table(target_table)
result_df.printSchema()

print(f"Curated records: {result_df.count()}")
display(result_df.orderBy("installment_id"))