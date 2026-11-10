# Workspace configuration

This guide identifies the values needed to use the exported notebooks, saved SQL queries, dashboard, and final job configuration in another Databricks workspace.

## 1. Preserve the project paths

Each notebook has a rendered `.ipynb` preview with the saved output available in the supplied HTML export. The adjacent `.py` or `.sql` file is its source export. Import one version of each notebook and verify the resulting notebook path before assigning it to a job task.

Import the assets from `00_setup/`, `01_tasks/`, `02_validation/`, and `03_dashboards/` into a workspace project folder named `fintech-lakeflow-orchestration`.

Retain the task notebook names and folder names so that they can be referenced by the job. Notebook filenames in GitHub include language extensions; verify the actual imported notebook paths when assigning them to job tasks.

The saved SQL queries are provided as `.sql` files. Create saved queries in Databricks SQL using their SQL contents. In particular, the `ingesting_customers` task references the saved query exported as `01_tasks/02_ingesting_customers_sql.sql`.

## 2. Replace the job template placeholders

The only job template is [`../jobs/fintech_credit_recovery_monitoring_job.json`](../jobs/fintech_credit_recovery_monitoring_job.json).

| Placeholder | Value to supply | Where it is used |
| --- | --- | --- |
| `<WORKSPACE_PROJECT_ROOT>` | The absolute path to your workspace project folder, without a trailing slash | Eight notebook task paths, including the nested state-report task. |
| `<SQL_WAREHOUSE_ID>` | The identifier of your serverless or Pro SQL warehouse | Customer ingestion and dashboard refresh. |
| `<CUSTOMERS_QUERY_ID>` | The identifier of your saved customer ingestion SQL query | The `ingesting_customers` SQL task. |
| `<DASHBOARD_ID>` | The identifier of your imported and published dashboard | The `refreshing_fintech_dashboard` task. |

For example, the project root could be `/Workspace/Users/your-user/fintech-lakeflow-orchestration`. Use the path that exists in your workspace.

Replace every occurrence of each placeholder before submitting the JSON to a job creation or update operation. The angle-bracket strings are documentation placeholders, not environment variables or native Databricks substitutions.

Retain `{{input}}` and `{{tasks.customers_installments_summary.values.has_duplicates}}`. These are Databricks dynamic value references used by the loop and condition.

The JSON can also serve as a configuration reference when creating the job in the UI. Select your own notebook paths, saved query, warehouse, and dashboard in the corresponding task settings.

## 3. Keep catalog and schema references consistent

The fixture uses the `workspace` catalog and `fintech_jobs` schema. The saved job parameters use these same defaults:

| Parameter | Saved default |
| --- | --- |
| `catalog` | `workspace` |
| `schema` | `fintech_jobs` |
| `simulate_duplicate` | `false` |

Most Python tasks read catalog and schema widgets. The following assets also contain literal `workspace.fintech_jobs` references:

- `00_setup/00_environment_setup.sql`
- `01_tasks/01_ingesting_applications.py`
- `01_tasks/02_ingesting_customers_sql.sql`
- `02_validation/01_validate_customers_staging_sql.sql`
- `02_validation/02_query_staging_tables_sql.sql`
- Dataset queries in `03_dashboards/fintech_credit_operations_dashboard.lvdash.json`

If you choose another catalog or schema, update these literal references, the volume paths in environment setup, and the job parameters together. Changing the job parameters alone does not retarget every asset.

## 4. Prepare the source data and dashboard resources

The environment setup notebook creates five synthetic customers, six applications, and six installments. Run the setup in your project environment before executing the processing tasks.

Installment ingestion reads CSV files from `/Volumes/<catalog>/<schema>/trigger_storage_location/`. For the expected fixture counts, that directory must contain one copy of the six installment records. The setup notebook also contains file-arrival exercises; repeated setup or test-file creation can leave additional CSV copies in the directory. Inspect its contents before repeating the workflow.

The final job JSON has no saved schedule or file-arrival trigger. The file-arrival sections in the setup notebook document an earlier exercise.

The dashboard datasets read the application detail, state summary, and curated installment output tables. Initialize the data-processing tasks and their output tables before validating and publishing the dashboard in a fresh workspace. Select your SQL warehouse, publish the imported dashboard, and use its identifier in the final refresh task.

Both the person configuring the job and the job's run identity need the appropriate access to the project assets, data, volume, and compute resources.

## 5. Select your own monitoring identifiers

`02_validation/05_monitor_fintech_job_runs.py` uses placeholders for monitoring identifiers:

| Widget | Placeholder |
| --- | --- |
| `workspace_id` | `<WORKSPACE_ID>` |
| `job_id` | `<JOB_ID>` |
| `job_run_id` | `<JOB_RUN_ID>` |

Set these widgets to identifiers from your own execution history. Also replace `<JOB_ID>` in the initial job-discovery SQL query; that query appears before the widget configuration cell. The placeholders do not resolve automatically.

Access to the relevant `system.lakeflow` tables is required for the monitoring notebook. The recorded outcomes and durations describe the original validation run; importing the project does not recreate that run history. Publish your imported dashboard to obtain a link for your own workspace.

## References

- [Import and export Databricks notebooks](https://docs.databricks.com/aws/en/notebooks/notebook-export-import)
- [SQL task for jobs](https://docs.databricks.com/aws/en/jobs/sql)
- [Dashboard task for jobs](https://docs.databricks.com/aws/en/jobs/tasks/dashboard)
- [Automate job creation and management](https://docs.databricks.com/aws/en/jobs/automate)
