# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC # Notebook Task — Credit Installments Ingestion
# MAGIC
# MAGIC **Objective:** Read synthetic credit installment records from CSV files in a Unity Catalog volume and persist them in a staging Delta table using PySpark.
# MAGIC
# MAGIC ## Why?
# MAGIC
# MAGIC **For our project:** Creates `installments_staging` as the third ingestion output, using `catalog` and `schema` task parameters. Lakeflow Jobs will execute this notebook within a job configured with a file arrival trigger.
# MAGIC
# MAGIC **For Data Engineering:** Demonstrates parameterized ingestion, governed file access through Unity Catalog, and separation between data processing and workflow orchestration.

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC #### 01 — Configure Parameters and Define the Volume Path
# MAGIC
# MAGIC Read the catalog and schema parameters and build the volume path used by the file arrival trigger. Defaults allow manual validation before configuring the task parameters.

# COMMAND ----------

# Define defaults for manual notebook execution.
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "fintech_jobs")

# Read the current parameter values.
my_catalog = dbutils.widgets.get("catalog")
my_schema = dbutils.widgets.get("schema")

# Build the Unity Catalog volume path.
my_volume_path = (
    f"/Volumes/{my_catalog}/{my_schema}/trigger_storage_location/"
)

# Display the configuration for validation.
print(my_catalog)
print(my_schema)
print(my_volume_path)

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC #### 02 — Ingest Credit Installments into Staging
# MAGIC
# MAGIC Read installment CSV files from the configured volume and overwrite the staging Delta table with their contents.

# COMMAND ----------

# Read installment CSV files, using the first row as column headers.
installments_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .load(my_volume_path)
)

# Define the staging table using the notebook parameters.
target_table = f"{my_catalog}.{my_schema}.installments_staging"

# Persist the records as a Delta table.
(
    installments_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(target_table)
)

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC #### 03 — Preview Credit Installments
# MAGIC
# MAGIC Display the records loaded from the CSV files to inspect the ingestion result.

# COMMAND ----------

# Display the installment records loaded during ingestion.
display(installments_df)