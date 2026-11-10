-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Environment Setup — Fintech Lakeflow Jobs
-- MAGIC
-- MAGIC **Objective:** Prepare the Databricks environment for a fintech credit orchestration project by creating a dedicated Unity Catalog schema and three Delta tables containing synthetic customer, credit application, and installment data.
-- MAGIC
-- MAGIC ## Why?
-- MAGIC
-- MAGIC **For our project:** Provides the source tables required to implement and test independent tasks, dependencies, parameterization, and workflow orchestration using Lakeflow Jobs.
-- MAGIC
-- MAGIC **For Data Engineering:** Demonstrates environment preparation, data organization with Unity Catalog, Delta table creation, and the separation of setup operations from orchestrated data processing.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 01 — Create Project Schema
-- MAGIC
-- MAGIC Create the `fintech_jobs` schema in the `workspace` catalog to organize the project's Delta tables.
-- MAGIC
-- MAGIC Verify that the schema exists by listing the available schemas in Unity Catalog.

-- COMMAND ----------

-- Create the schema for the Fintech Lakeflow orchestration project if it does not already exist.
CREATE SCHEMA IF NOT EXISTS workspace.fintech_jobs;

-- List all schemas in the workspace catalog to verify that fintech_jobs was created.
SHOW SCHEMAS IN workspace;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 02 — Create Credit Customers Source Table
-- MAGIC
-- MAGIC Create the `credit_customers` Delta table and insert five synthetic customer records.
-- MAGIC
-- MAGIC Each customer has a unique identifier, name, state, and registration date.
-- MAGIC
-- MAGIC Validate the inserted records using a SQL query.

-- COMMAND ----------

-- Create the source table for fintech credit customers.
CREATE OR REPLACE TABLE workspace.fintech_jobs.credit_customers (
    customer_id BIGINT,
    customer_name STRING,
    state STRING,
    registration_date DATE
)
USING DELTA;

-- Insert five synthetic customer records.
INSERT INTO workspace.fintech_jobs.credit_customers
VALUES
    (101, 'Ana Ribeiro', 'PE', DATE '2026-10-01'),
    (102, 'Carlos Lima', 'SP', DATE '2026-10-02'),
    (103, 'Maria Costa', 'RJ', DATE '2026-10-03'),
    (104, 'Pedro Alves', 'MG', DATE '2026-10-04'),
    (105, 'Julia Santos', 'BA', DATE '2026-10-05');

-- Verify the inserted records.
SELECT *
FROM workspace.fintech_jobs.credit_customers
ORDER BY customer_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 03 — Create Credit Applications Source Table
-- MAGIC
-- MAGIC Create the `credit_applications` Delta table and insert six synthetic credit applications associated with existing customers.
-- MAGIC
-- MAGIC Each application contains a unique identifier, customer reference, application date, requested amount, and approval status.
-- MAGIC
-- MAGIC The data includes approved, pending, and rejected applications.
-- MAGIC
-- MAGIC Validate the inserted records using a SQL query.

-- COMMAND ----------

-- Create the source table for fintech credit applications.
CREATE OR REPLACE TABLE workspace.fintech_jobs.credit_applications (
    application_id BIGINT,
    customer_id BIGINT,
    application_date DATE,
    requested_amount DECIMAL(12,2),
    status STRING
)
USING DELTA;

-- Insert six synthetic credit applications.
INSERT INTO workspace.fintech_jobs.credit_applications
VALUES
    (2001, 101, DATE '2026-10-06', 3500.00, 'APPROVED'),
    (2002, 102, DATE '2026-10-07', 8200.00, 'PENDING'),
    (2003, 103, DATE '2026-10-08', 5000.00, 'REJECTED'),
    (2004, 104, DATE '2026-10-09', 12000.00, 'APPROVED'),
    (2005, 105, DATE '2026-10-10', 2600.00, 'APPROVED'),
    (2006, 101, DATE '2026-10-11', 4500.00, 'PENDING');

-- Verify the inserted applications.
SELECT *
FROM workspace.fintech_jobs.credit_applications
ORDER BY application_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 04 — Create Credit Installments Source Table
-- MAGIC
-- MAGIC Create the `credit_installments` Delta table and insert six synthetic installment records associated with approved credit applications.
-- MAGIC
-- MAGIC Each installment contains an identifier, application reference, customer reference, due date, amount due, and payment status.
-- MAGIC
-- MAGIC The dataset includes paid and pending installments.
-- MAGIC
-- MAGIC Validate the inserted records using a SQL query.

-- COMMAND ----------

-- Create the source table for fintech credit installments.
CREATE OR REPLACE TABLE workspace.fintech_jobs.credit_installments (
    installment_id BIGINT,
    application_id BIGINT,
    customer_id BIGINT,
    due_date DATE,
    amount_due DECIMAL(12,2),
    payment_status STRING
)
USING DELTA;

-- Insert six synthetic installments for approved credit applications.
INSERT INTO workspace.fintech_jobs.credit_installments
VALUES
    (3001, 2001, 101, DATE '2026-10-20', 1750.00, 'PAID'),
    (3002, 2001, 101, DATE '2026-11-20', 1750.00, 'PENDING'),
    (3003, 2004, 104, DATE '2026-10-25', 4000.00, 'PAID'),
    (3004, 2004, 104, DATE '2026-11-25', 4000.00, 'PENDING'),
    (3005, 2004, 104, DATE '2026-12-25', 4000.00, 'PENDING'),
    (3006, 2005, 105, DATE '2026-11-10', 2600.00, 'PENDING');

-- Verify the inserted installments.
SELECT *
FROM workspace.fintech_jobs.credit_installments
ORDER BY installment_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create the File Arrival Volume
-- MAGIC
-- MAGIC **Objective:** Create a volume to store installment CSV files.
-- MAGIC
-- MAGIC **Why?** This volume provides the input location for the ingestion notebook and the path that the file arrival trigger will monitor.

-- COMMAND ----------

-- Create the managed volume for installment CSV files.
CREATE VOLUME IF NOT EXISTS
    workspace.fintech_jobs.trigger_storage_location;

-- Verify that the volume exists in the project schema.
SHOW VOLUMES IN workspace.fintech_jobs;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Prepare the Credit Installments CSV
-- MAGIC
-- MAGIC **Objective:** Export the existing synthetic installment records to a CSV in the project volume.
-- MAGIC
-- MAGIC **Why?** Provides a reproducible file input for testing the ingestion notebook.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Define the destination file inside the volume.
-- MAGIC volume_path = "/Volumes/workspace/fintech_jobs/trigger_storage_location/"
-- MAGIC csv_path = f"{volume_path}credit_installments.csv"
-- MAGIC
-- MAGIC # Export the small dataset as one CSV with headers.
-- MAGIC installments_pdf = (
-- MAGIC     spark.table("workspace.fintech_jobs.credit_installments").toPandas()
-- MAGIC )
-- MAGIC installments_pdf.to_csv(csv_path, index=False)
-- MAGIC
-- MAGIC # Read the exported CSV to verify access and record count.
-- MAGIC csv_df = spark.read.option("header", "true").csv(csv_path)
-- MAGIC
-- MAGIC # Display the results for validation.
-- MAGIC print(f"Source records: {len(installments_pdf)}")
-- MAGIC print(f"CSV records: {csv_df.count()}")
-- MAGIC print(f"CSV path: {csv_path}")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Validate the File Arrival Trigger
-- MAGIC
-- MAGIC **Objective:** Automatically execute `fintech_credit_scheduling_job`
-- MAGIC when a new file arrives in the monitored volume.
-- MAGIC
-- MAGIC **Data Engineering value:** Connect file delivery to workflow execution,
-- MAGIC reducing manual intervention and making ingestion observable.
-- MAGIC
-- MAGIC **Expected result:** A Job run launched “By file arrival,”
-- MAGIC with all three tasks showing “Succeeded.”

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Simulate a New File Arrival
-- MAGIC
-- MAGIC **Objective:** Test automatic Job execution by giving the existing
-- MAGIC installment CSV a unique filename.
-- MAGIC
-- MAGIC **Why:** Validate file arrival detection while preserving one input
-- MAGIC file and the original six records.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from uuid import uuid4
-- MAGIC
-- MAGIC # Define the existing file and a unique destination filename.
-- MAGIC volume_path = "/Volumes/workspace/fintech_jobs/trigger_storage_location/"
-- MAGIC source_path = f"{volume_path}credit_installments.csv"
-- MAGIC arrival_path = f"{volume_path}credit_installments_{uuid4().hex}.csv"
-- MAGIC
-- MAGIC # Rename the test file without retaining a second copy.
-- MAGIC moved = dbutils.fs.mv(source_path, arrival_path)
-- MAGIC
-- MAGIC # Verify the operation and the volume contents.
-- MAGIC print(f"File renamed: {moved}")
-- MAGIC print(f"New file: {arrival_path}")
-- MAGIC display(dbutils.fs.ls(volume_path))