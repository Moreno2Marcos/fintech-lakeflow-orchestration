# Databricks notebook source
# MAGIC %md
# MAGIC # Dynamic Fintech Workloads — Dependencies, Conditions and Iteration
# MAGIC
# MAGIC **Objective:** Extend our ingestion workflow with dependent joins, conditional duplicate handling, and state-based credit application reports.
# MAGIC
# MAGIC ## Why?
# MAGIC
# MAGIC **For our project:** Combine staged customers, credit applications, and installments, then create reusable processing tasks and reports.
# MAGIC
# MAGIC **For Data Engineering:** Control execution order, pass runtime task values, branch based on data quality, and reuse one parameterized notebook for multiple states.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 01 — Verify Existing Staging Tables
# MAGIC
# MAGIC Read the catalog and schema parameters, then inspect the staging table schemas and record counts to confirm that the inputs are ready for the dependent processing tasks.

# COMMAND ----------

# Define defaults for manual notebook execution.
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "fintech_jobs")

# Read the current parameter values.
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
namespace = f"{catalog}.{schema}"

# Inspect the existing staging tables.
table_names = [
    "customers_staging",
    "applications_staging",
    "installments_staging",
]

for table_name in table_names:
    full_table_name = f"{namespace}.{table_name}"
    df = spark.table(full_table_name)

    print(f"{full_table_name}: {df.count()} records")
    df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 02 — Configure the Dynamic Fintech Job
# MAGIC
# MAGIC Configure `fintech_credit_dynamic_job` to orchestrate ingestion, joins, conditional duplicate handling, and state-based reporting.
# MAGIC
# MAGIC Use task dependencies to wait for required inputs, an If/else condition to control duplicate removal, and a For each task to generate application reports for each state.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 03 — Configure Conditional Duplicate Handling
# MAGIC
# MAGIC Use the `has_duplicates` value published by `customers_installments_summary` to determine whether duplicate removal is required.
# MAGIC
# MAGIC A True result selects duplicate removal. A False result proceeds directly to transformation.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 04 — Connect the Conditional Branches
# MAGIC
# MAGIC When duplicates are detected, remove them before transformation.
# MAGIC Otherwise, proceed directly to transformation.
# MAGIC
# MAGIC The shared transformation task uses `None failed` to continue
# MAGIC after either branch, provided no dependency has failed.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 05 — Configure State Report Iteration
# MAGIC
# MAGIC Use a For each task to generate application reports for PE, SP,
# MAGIC RJ, MG, and BA with the same notebook.
# MAGIC
# MAGIC Pass each state to the nested notebook through its `state`
# MAGIC parameter. Set concurrency to 1 to process states sequentially.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 06 — Execute the Dynamic Fintech Job
# MAGIC
# MAGIC Run the complete workflow with `simulate_duplicate=false`.
# MAGIC
# MAGIC Verify that the condition selects the False branch, transformation
# MAGIC completes without duplicate removal, and all five state iterations
# MAGIC succeed.
# MAGIC
# MAGIC For a controlled True-branch test, override `simulate_duplicate`
# MAGIC to `true` for a single Job run. Expect 7 joined rows, 6 distinct
# MAGIC rows, and one duplicate removed before transformation.
# MAGIC Keep the saved Job default as `false`.
# MAGIC
# MAGIC The controlled True-branch test passed.
# MAGIC Duplicate detection identified 7 rows with 6 distinct records.
# MAGIC Duplicate removal removed exactly one row, transformation produced
# MAGIC 6 curated records, and all five state iterations succeeded.
