# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC # Notebook Task — Credit Applications Ingestion
# MAGIC
# MAGIC **Objective:** Read synthetic fintech credit applications from an existing Delta table and persist the records into a staging Delta table using Databricks and PySpark.
# MAGIC
# MAGIC ## Why?
# MAGIC
# MAGIC **For our project:** Creates the `applications_staging` table from `credit_applications`, providing a simple processing task that Lakeflow Jobs can execute and monitor.
# MAGIC
# MAGIC **For Data Engineering:** Demonstrates the separation between data processing logic and workflow orchestration, using reproducible Delta table operations.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 01 — Read Source Credit Applications
# MAGIC
# MAGIC Read the existing credit applications from Unity Catalog using PySpark. The source table contains six synthetic credit applications.

# COMMAND ----------

# Define the source and target Delta tables.
source_table = "workspace.fintech_jobs.credit_applications"
target_table = "workspace.fintech_jobs.applications_staging"

# Read the existing credit applications from Unity Catalog.
applications_df = spark.table(source_table)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 02 — Write Staging Delta Table
# MAGIC
# MAGIC Write the source records to `applications_staging` using Delta Lake. Overwrite mode replaces the existing target data, preventing duplicate accumulation during repeated executions.

# COMMAND ----------

# Create or overwrite the target Delta table.
applications_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(target_table)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 03 — Validate Data Load
# MAGIC
# MAGIC Compare the source and target record counts to verify that all six credit applications were loaded into the staging table.

# COMMAND ----------

# Confirm that the task processed the expected records.
print(f"Source records: {applications_df.count()}")
print(f"Target records: {spark.table(target_table).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <div style="border: 1px solid #94A3B8; border-left: 4px solid #2563EB; border-radius: 8px; padding: 16px; margin: 12px 0;">
# MAGIC
# MAGIC   <h3>💡 Engineering Note — Staging vs. Bronze</h3>
# MAGIC
# MAGIC   <p>The <code>applications_staging</code> table stores an intermediate copy of the source data without transformations.</p>
# MAGIC
# MAGIC   <p>Although this resembles a Bronze layer, staging tables primarily support intermediate data processing and do not necessarily preserve historical raw data.</p>
# MAGIC
# MAGIC   <p><strong>In this project:</strong> Staging prepares a simple table-to-table workload for Lakeflow Jobs orchestration, without implementing a Medallion Architecture.</p>
# MAGIC
# MAGIC   <hr>
# MAGIC
# MAGIC   <p><strong>References — Official Documentation:</strong></p>
# MAGIC
# MAGIC   <ul>
# MAGIC     <li>
# MAGIC       <a href="https://learn.microsoft.com/en-us/fabric/data-warehouse/dimensional-modeling-load-tables">
# MAGIC         Microsoft Learn — Load Tables in a Dimensional Model (Stage Data)
# MAGIC       </a>
# MAGIC     </li>
# MAGIC     <li>
# MAGIC       <a href="https://docs.databricks.com/aws/en/lakehouse/medallion">
# MAGIC         Databricks — Medallion Architecture (Bronze Layer)
# MAGIC       </a>
# MAGIC     </li>
# MAGIC   </ul>
# MAGIC
# MAGIC </div>
# MAGIC