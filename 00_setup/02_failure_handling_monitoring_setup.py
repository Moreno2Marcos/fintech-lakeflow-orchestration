# Databricks notebook source
# MAGIC %md
# MAGIC # Fintech Workloads — Failure Recovery and Performance Monitoring
# MAGIC
# MAGIC **Objective:** Extend our fintech workflow with a controlled task failure, selective repair, a published dashboard, and operational monitoring.
# MAGIC
# MAGIC ## Why?
# MAGIC
# MAGIC **For our project:** Recover an application transformation without repeating successful ingestion and reporting tasks, then refresh a dashboard after both reporting branches finish.
# MAGIC
# MAGIC **For Data Engineering:** Diagnose failed tasks, distinguish repair from a new run, design repeatable writes, inspect query performance, and query job execution history.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 01 — Prepare the Recovery Job
# MAGIC
# MAGIC Clone the validated dynamic workflow to reuse its task dependencies
# MAGIC and five state reports for the failure recovery and monitoring exercise.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 02 — Create the Application Transformation
# MAGIC
# MAGIC Add a transformation after all five state reports finish.
# MAGIC Combine their records into an application detail table and a
# MAGIC state summary table for the dashboard.
# MAGIC
# MAGIC The application transformation depends on the For each container
# MAGIC and runs after all five state reports succeed. For the controlled
# MAGIC failure test, automatic retries are disabled on this task.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 03 — Execute and Diagnose the Controlled Failure
# MAGIC
# MAGIC Run the complete recovery job and inspect the failed application
# MAGIC transformation. Identify the missing column while confirming that
# MAGIC the upstream tasks succeeded.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 04 — Correct the Code and Repair the Same Run
# MAGIC
# MAGIC Correct the customer-name reference in the source notebook.
# MAGIC Repair the failed application transformation within the existing
# MAGIC job run, preserving successful upstream work.
# MAGIC
# MAGIC Repair 1 succeeded within the original parent job run.
# MAGIC Only the failed application transformation was repeated.
# MAGIC It produced 6 application records and 5 state-summary records.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 05 — Validate the Repaired Outputs
# MAGIC
# MAGIC Compare the repaired application details and state summary
# MAGIC with the source data. Verify the existing installment totals
# MAGIC before creating the dashboard.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 06 — Create and Publish the Fintech Dashboard
# MAGIC
# MAGIC Create a dashboard using the validated application and installment
# MAGIC tables. Configure its datasets and visualizations, then publish it
# MAGIC for refresh through the recovery job.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 07 — Add the Dashboard Refresh Task
# MAGIC
# MAGIC Add refreshing_fintech_dashboard to the recovery job.
# MAGIC Configure it to refresh fintech_credit_operations_dashboard
# MAGIC using Serverless Starter Warehouse.
# MAGIC
# MAGIC The task depends on transforming_applications_by_state and
# MAGIC transforming_customers_installments, with Run if set to All succeeded.
# MAGIC This ensures both data branches finish successfully before the
# MAGIC dashboard refreshes.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 08 — Inspect Timeline and Query Performance
# MAGIC
# MAGIC The final job run succeeded, including the dashboard refresh.
# MAGIC
# MAGIC The timeline shows overlapping ingestion tasks and a dashboard refresh
# MAGIC that waits for both transformation branches.
# MAGIC
# MAGIC Expand transforming_applications_by_state and inspect the state summary
# MAGIC write query. Review its duration, rows read and written, execution plan,
# MAGIC and longest operators.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 09 — Query Job Metadata and Execution History
# MAGIC
# MAGIC Created 02_validation/05_monitor_fintech_job_runs and verified access to
# MAGIC the jobs, job_run_timeline, and job_task_run_timeline system tables.
# MAGIC
# MAGIC The task history preserves the failed transformation and successful repair
# MAGIC as separate task runs under the same parent job run. The final run also
# MAGIC confirms successful transformation and dashboard refresh.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 10 — Completion Record
# MAGIC
# MAGIC | Evidence | Recorded result |
# MAGIC | --- | --- |
# MAGIC | Failure and repair | Same parent job run |
# MAGIC | Failed task attempt | FAILED, 19.664 seconds |
# MAGIC | Controlled error | UNRESOLVED_COLUMN.WITH_SUGGESTION: customer |
# MAGIC | Persistent correction | Reference customer_name |
# MAGIC | Repair 1 task attempt | SUCCEEDED, 22.076 seconds |
# MAGIC | Successful upstream tasks preserved | Verified |
# MAGIC | Final dashboard job run | SUCCEEDED |
# MAGIC | Final application transformation | SUCCEEDED, 22.841 seconds |
# MAGIC | Dashboard refresh | SUCCEEDED, 21.304 seconds |
# MAGIC | Validated application outputs | 6 detail records, 5 state summaries |
# MAGIC | Total requested amount | 35,800.00 |
# MAGIC | Paid installments | 2 records, 5,750.00 |
# MAGIC | Pending installments | 4 records, 12,350.00 |
# MAGIC | Profiled summary write | 4.408 seconds; 6 rows read, 5 rows written |
# MAGIC | System-table monitoring | Passed |
# MAGIC | Optional billing inspection | Not included |
# MAGIC
# MAGIC Final job parameters remain catalog=workspace, schema=fintech_jobs,
# MAGIC and simulate_duplicate=false.
# MAGIC
# MAGIC Serverless auto-optimization was restored on
# MAGIC transforming_applications_by_state after the controlled failure exercise.
# MAGIC
# MAGIC Import and publish the dashboard definition in your own workspace to obtain its dashboard link.
