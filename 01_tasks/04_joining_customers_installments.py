# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook Task — Join Customers and Installments
# MAGIC
# MAGIC **Objective:** Join staged installments to customer details, save the joined Delta table, and publish a duplicate flag for the workflow.
# MAGIC
# MAGIC ## Why?
# MAGIC
# MAGIC **For our project:** Add customer names and states to installment records.
# MAGIC
# MAGIC **For Data Engineering:** Demonstrate a task that depends on two inputs and passes a data quality result to a downstream condition.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 01 — Configure Notebook Parameters
# MAGIC
# MAGIC Read the catalog and schema parameters. Keep duplicate simulation disabled for the initial run; we will use it later to test the conditional branch.

# COMMAND ----------

# Define defaults for manual execution.
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "fintech_jobs")
dbutils.widgets.text("simulate_duplicate", "false")

# Read the current parameter values.
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
namespace = f"{catalog}.{schema}"

simulate_duplicate = (
    dbutils.widgets.get("simulate_duplicate").strip().lower()
)
assert simulate_duplicate in {"true", "false"}

# COMMAND ----------

# MAGIC %md
# MAGIC #### 02 — Join Installments and Check for Duplicates
# MAGIC
# MAGIC Join installments to customer details using `customer_id` and save the result as a Delta table.
# MAGIC
# MAGIC Compare the total row count with the distinct full-row count and publish the Boolean result as `has_duplicates` for the downstream If/else task.

# COMMAND ----------

# Enrich installments with customer details.
joined_df = spark.sql(f"""
    SELECT
        i.installment_id,
        i.application_id,
        c.customer_id,
        c.customer_name,
        c.state,
        i.due_date,
        i.amount_due,
        i.payment_status
    FROM {namespace}.installments_staging AS i
    INNER JOIN {namespace}.customers_staging AS c
        ON CAST(i.customer_id AS BIGINT) = c.customer_id
""")

# Add one identical row only during the controlled duplicate test.
if simulate_duplicate == "true":
    joined_df = joined_df.unionByName(joined_df.limit(1))

# Persist the joined records.
target_table = f"{namespace}.customers_installments_silver"

joined_df.write.format("delta").mode("overwrite").saveAsTable(
    target_table
)

# Check for identical full-row duplicates.
stored_df = spark.table(target_table)
total_count = stored_df.count()
distinct_count = stored_df.dropDuplicates().count()
has_duplicates = total_count > distinct_count

# Publish the result for downstream Job tasks.
dbutils.jobs.taskValues.set(
    key="has_duplicates",
    value=has_duplicates
)

print(f"Total rows: {total_count}")
print(f"Distinct rows: {distinct_count}")
print(f"Has duplicates: {has_duplicates}")

display(stored_df.orderBy("installment_id"))