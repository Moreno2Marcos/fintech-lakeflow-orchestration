
-- SQL Query Task — Credit Customers Ingestion
--
-- Objective:
-- Copy synthetic fintech customer records from an existing
-- Delta table into a staging Delta table using Databricks SQL.
--
-- Why?
-- For our project: Prepare a SQL processing task that
-- Lakeflow Jobs can execute and monitor.
--
-- For Data Engineering: Demonstrate how SQL workloads
-- can be integrated into an orchestration workflow.

-- Create or replace the staging table using source data.
CREATE OR REPLACE TABLE workspace.fintech_jobs.customers_staging
AS
SELECT *
FROM workspace.fintech_jobs.credit_customers;
