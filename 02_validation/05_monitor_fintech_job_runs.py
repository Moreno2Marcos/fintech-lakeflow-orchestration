# Databricks notebook source
# MAGIC %md
# MAGIC # Monitor Fintech Job Runs
# MAGIC
# MAGIC **Objective:** Inspect operational history for
# MAGIC fintech_credit_recovery_monitoring_job.
# MAGIC
# MAGIC ## Why?
# MAGIC
# MAGIC **For our project:** Compare the repaired run with the successful dashboard
# MAGIC run and document the pipeline's execution history.
# MAGIC
# MAGIC **For Data Engineering:** Practice querying operational system tables to
# MAGIC investigate failures, distinguish task attempts, and analyze execution durations.

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN system;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN system.lakeflow;

# COMMAND ----------

# MAGIC %md
# MAGIC #### 01 — Inspect Metadata Tables and Identify the Recovery Job
# MAGIC
# MAGIC Inspect the table columns and retrieve the latest active job definition.
# MAGIC Use its workspace and job identifiers in subsequent monitoring queries.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Job metadata columns
# MAGIC DESCRIBE TABLE system.lakeflow.jobs;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Task execution columns
# MAGIC DESCRIBE TABLE system.lakeflow.job_task_run_timeline;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Identify the recovery job
# MAGIC WITH latest_jobs AS (
# MAGIC     SELECT
# MAGIC         workspace_id,
# MAGIC         job_id,
# MAGIC         name,
# MAGIC         change_time,
# MAGIC         delete_time,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY workspace_id, job_id
# MAGIC             ORDER BY change_time DESC
# MAGIC         ) AS row_num
# MAGIC     FROM system.lakeflow.jobs
# MAGIC     WHERE job_id = '<JOB_ID>'
# MAGIC )
# MAGIC SELECT
# MAGIC     workspace_id,
# MAGIC     job_id,
# MAGIC     name,
# MAGIC     change_time
# MAGIC FROM latest_jobs
# MAGIC WHERE row_num = 1
# MAGIC   AND delete_time IS NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC #### 02 — Configure Monitoring Parameters
# MAGIC
# MAGIC Select the workspace, job, and parent job run to inspect.
# MAGIC Start with the run used for the controlled failure and repair.

# COMMAND ----------

dbutils.widgets.text("workspace_id", "<WORKSPACE_ID>")
dbutils.widgets.text("job_id", "<JOB_ID>")
dbutils.widgets.text("job_run_id", "<JOB_RUN_ID>")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 03 — Inspect Task Execution History
# MAGIC
# MAGIC Join the current job definition to its task execution history.
# MAGIC Identify the failed application transformation and its successful repair.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH latest_job AS (
# MAGIC     SELECT
# MAGIC         workspace_id,
# MAGIC         job_id,
# MAGIC         name,
# MAGIC         delete_time,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY workspace_id, job_id
# MAGIC             ORDER BY change_time DESC
# MAGIC         ) AS row_num
# MAGIC     FROM system.lakeflow.jobs
# MAGIC     WHERE workspace_id = :workspace_id
# MAGIC       AND job_id = :job_id
# MAGIC )
# MAGIC SELECT
# MAGIC     j.name AS current_job_name,
# MAGIC     t.job_run_id,
# MAGIC     t.task_key,
# MAGIC     t.run_id AS task_run_id,
# MAGIC     t.period_start_time,
# MAGIC     t.period_end_time,
# MAGIC     t.result_state,
# MAGIC     t.termination_code
# MAGIC FROM system.lakeflow.job_task_run_timeline AS t
# MAGIC JOIN latest_job AS j
# MAGIC     ON t.workspace_id = j.workspace_id
# MAGIC    AND t.job_id = j.job_id
# MAGIC    AND j.row_num = 1
# MAGIC    AND j.delete_time IS NULL
# MAGIC WHERE t.workspace_id = :workspace_id
# MAGIC   AND t.job_id = :job_id
# MAGIC   AND t.job_run_id = :job_run_id
# MAGIC ORDER BY t.period_start_time, t.task_key, t.run_id;

# COMMAND ----------

# MAGIC %md
# MAGIC #### 04 — Calculate Task Attempt Durations
# MAGIC
# MAGIC Combine timeline slices for each task run and calculate its duration.
# MAGIC Keep separate task run IDs so the failed attempt and repair remain distinct.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     task_key,
# MAGIC     run_id AS task_run_id,
# MAGIC     MIN(period_start_time) AS task_start,
# MAGIC     MAX(period_end_time) AS task_end,
# MAGIC     ROUND(
# MAGIC         SUM(
# MAGIC             TIMESTAMPDIFF(
# MAGIC                 MICROSECOND,
# MAGIC                 period_start_time,
# MAGIC                 period_end_time
# MAGIC             )
# MAGIC         ) / 1000000.0,
# MAGIC         3
# MAGIC     ) AS task_timeline_seconds,
# MAGIC     MAX_BY(result_state, period_end_time) AS result_state
# MAGIC FROM system.lakeflow.job_task_run_timeline
# MAGIC WHERE workspace_id = :workspace_id
# MAGIC   AND job_id = :job_id
# MAGIC   AND job_run_id = :job_run_id
# MAGIC GROUP BY
# MAGIC     workspace_id,
# MAGIC     job_id,
# MAGIC     job_run_id,
# MAGIC     task_key,
# MAGIC     run_id
# MAGIC ORDER BY task_start, task_key, task_run_id;

# COMMAND ----------

# MAGIC %md
# MAGIC #### 05 — Inspect Job Completion Events
# MAGIC
# MAGIC Review completion events across this job's runs.
# MAGIC Compare the original failure, successful repair, and final dashboard run.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     run_id AS job_run_id,
# MAGIC     period_end_time AS terminal_event_time,
# MAGIC     result_state,
# MAGIC     termination_code
# MAGIC FROM system.lakeflow.job_run_timeline
# MAGIC WHERE workspace_id = :workspace_id
# MAGIC   AND job_id = :job_id
# MAGIC   AND result_state IS NOT NULL
# MAGIC ORDER BY terminal_event_time DESC
# MAGIC LIMIT 50;
