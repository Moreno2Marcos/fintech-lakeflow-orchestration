# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook Task — Join Customers and Credit Applications
# MAGIC
# MAGIC **Objective:** Combine staged credit applications with customer names and states.
# MAGIC
# MAGIC ## Why?
# MAGIC
# MAGIC **For our project:** Prepare application records for state-based reporting.
# MAGIC
# MAGIC **For Data Engineering:** Make the reporting task wait for both ingestion inputs and provide one reusable dataset to the For each task.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 01 — Configure Notebook Parameters
# MAGIC
# MAGIC Read the catalog and schema parameters to identify the tables used by this task.

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
# MAGIC #### 02 — Build the Joined Application Dataset
# MAGIC
# MAGIC Join staged applications to customer details using `customer_id` and save the result as a Delta table for state-based reporting.

# COMMAND ----------

# Enrich applications with customer names and states.
applications_df = spark.sql(f"""
    SELECT
        a.application_id,
        c.customer_id,
        c.customer_name,
        c.state,
        a.application_date,
        a.requested_amount,
        a.status
    FROM {namespace}.applications_staging AS a
    INNER JOIN {namespace}.customers_staging AS c
        ON a.customer_id = c.customer_id
""")

# Persist the joined application records.
target_table = f"{namespace}.customers_applications_silver"

applications_df.write.format("delta").mode("overwrite").saveAsTable(
    target_table
)

# Display the persisted result for validation.
stored_df = spark.table(target_table)

print(f"Application records: {stored_df.count()}")
display(stored_df.orderBy("application_id"))