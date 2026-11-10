# Fintech Lakeflow Orchestration

Fintech workflow orchestration with Databricks Lakeflow Jobs: conditional tasks, state-based iteration, failure recovery, monitoring, and dashboard refresh.

The project processes synthetic customer, credit application, and installment data with SQL and PySpark. Its final job coordinates ingestion, joins, conditional duplicate removal, five state reports, application and installment transformations, and a dashboard refresh.

## Open the formatted notebooks

The notebook previews below contain the Markdown cells, code, and saved outputs recovered from the supplied Databricks HTML export: **14 notebooks, 23 result tables, and 15 text outputs**.

| Section | Formatted notebooks |
| --- | --- |
| Setup | [Environment setup](00_setup/00_environment_setup.ipynb) · [Dynamic workloads](00_setup/01_dynamic_workloads_setup.ipynb) · [Failure recovery exercise](00_setup/02_failure_handling_monitoring_setup.ipynb) |
| Ingestion | [Applications](01_tasks/01_ingesting_applications.ipynb) · [Installments](01_tasks/03_ingesting_installments.ipynb) |
| Joins and duplicate handling | [Customer installments](01_tasks/04_joining_customers_installments.ipynb) · [Customer applications](01_tasks/05_joining_customers_applications.ipynb) · [Duplicate removal](01_tasks/06_dropping_duplicate_installments.ipynb) |
| Transformations | [Installments](01_tasks/07_transforming_customers_installments.ipynb) · [State reports](01_tasks/08_applications_by_state.ipynb) · [Application consolidation](01_tasks/09_transforming_applications_by_state.ipynb) |
| Validation and monitoring | [Dynamic workflow validation](02_validation/03_validate_dynamic_workflow_outputs.ipynb) · [Recovery output validation](02_validation/04_validate_recovery_monitoring_outputs.ipynb) · [Job run monitoring](02_validation/05_monitor_fintech_job_runs.ipynb) |

Open these `.ipynb` links for the notebook presentation. The adjacent `.py` and `.sql` files provide the source code. An `.ipynb` file is a notebook container; the environment setup notebook contains SQL cells, while the three saved SQL queries remain `.sql` files.

### Captured output coverage

The previews preserve all tables and text outputs embedded in the supplied export, in cell order. Workspace and run identifiers are replaced with consistent placeholders; result rows, amounts, statuses, and durations are retained.

- The export contains no saved execution output for `09_transforming_applications_by_state`. Its preview links to the captured validation results for the persisted tables.
- `02_failure_handling_monitoring_setup` is a Markdown exercise and completion record.
- The environment setup export contains a cached SQL parse error attached to a Markdown cell. That recorded error is retained and labeled as exported output.
- The notebook-only HTML export does not contain results for the three saved SQL queries.

## Project files

| Location | Contents | Purpose |
| --- | --- | --- |
| `00_setup/` | Three setup and exercise notebooks | Create the synthetic source data and explain the workflow exercises. |
| `01_tasks/` | Eight Python notebooks and one saved SQL query | Implement the processing tasks. |
| `02_validation/` | Three Python notebooks and two saved SQL queries | Validate records and totals, and inspect job execution history. |
| `03_dashboards/` | The exported `.lvdash.json` dashboard | Define the four datasets and dashboard visualizations. |
| `jobs/` | The final recovery and monitoring job JSON | Record task dependencies, branching, iteration, compute, and retry settings. |
| `docs/` | Workspace configuration notes | Explain the values required when reproducing the project. |

The `.py` and `.sql` notebook exports retain Databricks cell markers and Markdown cells. The saved SQL queries are provided as `.sql` files. The workflow uses Databricks services, including Spark, Delta tables, Unity Catalog volumes, and a SQL warehouse.

## Final job configuration

[`jobs/fintech_credit_recovery_monitoring_job.json`](jobs/fintech_credit_recovery_monitoring_job.json) is the single job configuration included in this repository. It has 11 top-level tasks and one nested task that runs for each of five states: PE, SP, RJ, MG, and BA.

The installment transformation uses `NONE_FAILED` after the conditional branches. The dashboard refresh waits for both final transformations with `ALL_SUCCESS`.

The saved job defaults are `catalog=workspace`, `schema=fintech_jobs`, and `simulate_duplicate=false`. The final configuration uses performance-optimized serverless compute and retains the restored serverless auto-optimization setting on the application transformation.

## Workspace configuration

Read [the workspace configuration guide](docs/workspace_configuration.md) before using the job template. Replace its four placeholders with paths and resource identifiers from your own workspace. These placeholders require explicit replacement; Databricks does not substitute them automatically.

The exported setup, queries, and dashboard use `workspace.fintech_jobs`. The exercise notes record the original outcomes and durations. Set the monitoring widgets to identifiers from your own execution history.
