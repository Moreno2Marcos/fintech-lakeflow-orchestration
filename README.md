# Fintech Lakeflow Orchestration

Fintech workflow orchestration with Databricks Lakeflow Jobs: conditional tasks, state-based iteration, failure recovery, monitoring, and dashboard refresh.

The project processes synthetic customer, credit application, and installment data with SQL and PySpark. Its final job coordinates ingestion, joins, conditional duplicate removal, five state reports, application and installment transformations, and a dashboard refresh.

## Project files

| Location | Contents | Purpose |
| --- | --- | --- |
| `00_setup/` | Three setup and exercise notebooks | Create the synthetic source data and explain the workflow exercises. |
| `01_tasks/` | Eight Python notebooks and one saved SQL query | Implement the processing tasks. |
| `02_validation/` | Three Python notebooks and two saved SQL queries | Validate records and totals, and inspect job execution history. |
| `03_dashboards/` | The exported `.lvdash.json` dashboard | Define the four datasets and dashboard visualizations. |
| `jobs/` | The final recovery and monitoring job JSON | Record task dependencies, branching, iteration, compute, and retry settings. |
| `docs/` | Workspace configuration notes | Explain the values required when reproducing the project. |

The `.py` and `.sql` notebook exports retain Databricks cell markers and Markdown cells. The `.dbquery.ipynb` files are saved SQL query exports. The workflow uses Databricks services, including Spark, Delta tables, Unity Catalog volumes, and a SQL warehouse.

## Final job configuration

[`jobs/fintech_credit_recovery_monitoring_job.json`](jobs/fintech_credit_recovery_monitoring_job.json) is the single job configuration included in this repository. It has 11 top-level tasks and one nested task that runs for each of five states: PE, SP, RJ, MG, and BA.

The installment transformation uses `NONE_FAILED` after the conditional branches. The dashboard refresh waits for both final transformations with `ALL_SUCCESS`.

The saved job defaults are `catalog=workspace`, `schema=fintech_jobs`, and `simulate_duplicate=false`. The final configuration uses performance-optimized serverless compute and retains the restored serverless auto-optimization setting on the application transformation.

## Workspace configuration

Read [the workspace configuration guide](docs/workspace_configuration.md) before using the job template. Replace its four placeholders with paths and resource identifiers from your own workspace. These placeholders require explicit replacement; Databricks does not substitute them automatically.

The exported setup, queries, and dashboard use `workspace.fintech_jobs`. The exercise notes record the original outcomes and durations. Set the monitoring widgets to identifiers from your own execution history.
