# Credit Operations Dashboard

This folder contains the presentation assets for the fintech credit workflow. The dashboard summarizes credit applications and installment payments using the curated tables produced by the Lakeflow Job. All records in this project are synthetic.

## Files

| File | Purpose |
| --- | --- |
| [Screenshot of the Dashboard.png](<Screenshot of the Dashboard.png>) | Static preview of the dashboard with the recorded project results. It allows readers to inspect the presentation directly on GitHub. |
| [fintech_credit_operations_dashboard.lvdash.json](fintech_credit_operations_dashboard.lvdash.json) | Exported Databricks AI/BI dashboard definition, containing the SQL datasets, visualization settings, and page layout. Import this file into Databricks to create an editable dashboard. |

## Dashboard preview

![Fintech credit operations dashboard showing application totals, application status, requested amounts by state, and installment payments](<Screenshot of the Dashboard.png>)

The screenshot records the dashboard at the time of capture. The interactive dashboard uses the data available in its Databricks workspace when refreshed.

## What the dashboard shows

| Component | Information displayed | Recorded result |
| --- | --- | --- |
| Total Applications | Number of credit applications | 6 applications |
| Total Requested Amount | Sum of application amounts | 35,800.00 |
| Applications by Status | Application count for each approval status | 3 approved, 2 pending, 1 rejected |
| Requested Amount by State | Requested amounts grouped by state | BA, MG, PE, RJ, and SP |
| Installments by Payment Status | Installment counts and amounts grouped by payment status | Paid: 2 installments totaling 5,750.00; pending: 4 totaling 12,350.00 |

These values describe the synthetic demonstration dataset.

## JSON definition and data sources

The JSON defines one page, **Credit Operations Overview**, with five widgets and four SQL datasets:

| SQL dataset | Source table | Used by |
| --- | --- | --- |
| `application_kpis` | `workspace.fintech_jobs.customers_applications_gold` | Total Applications and Total Requested Amount |
| `applications_by_state` | `workspace.fintech_jobs.customers_applications_state_summary_gold` | Requested Amount by State |
| `applications_by_status` | `workspace.fintech_jobs.customers_applications_gold` | Applications by Status |
| `installments_by_payment_status` | `workspace.fintech_jobs.customers_installments_curated` | Installments by Payment Status |

The source records are created and processed by the project notebooks. The JSON stores the dashboard configuration; the screenshot provides its visual evidence.

## Dashboard refresh in the workflow

The `refreshing_fintech_dashboard` task in the [final job configuration](../jobs/fintech_credit_recovery_monitoring_job.json) depends on both `transforming_applications_by_state` and `transforming_customers_installments`. Its `ALL_SUCCESS` condition requires both dependencies to succeed before the dashboard refresh runs.

This connects the reporting step to completion of the application and installment transformations.

## Use the dashboard in your workspace

1. Prepare the synthetic source data and run the processing tasks to create the three source tables listed above.
2. In Databricks, open the dashboards listing and select **Import dashboard from file**. Choose `fintech_credit_operations_dashboard.lvdash.json` and confirm the import.
3. Review the dataset SQL references. The export uses `workspace.fintech_jobs`; update the references if your catalog or schema differs. Select a SQL warehouse with access to these tables and verify the dataset results.
4. Publish the dashboard. Replace `<DASHBOARD_ID>` and `<SQL_WAREHOUSE_ID>` in the final job template with the identifiers from your workspace before configuring its refresh task.

See the [workspace configuration guide](../docs/workspace_configuration.md) for the remaining project settings and the official Databricks documentation for [dashboard import and export](https://docs.databricks.com/aws/en/dashboards/automate/import-export).
