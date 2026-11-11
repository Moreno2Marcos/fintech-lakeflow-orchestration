# Validation and monitoring

Open the `.ipynb` links below to view formatted Markdown, code cells, and the saved outputs recovered from the Databricks HTML export.

| Notebook | Saved outputs |
| --- | --- |
| [03_validate_dynamic_workflow_outputs](03_validate_dynamic_workflow_outputs.ipynb) | 2 tables, 2 text outputs |
| [04_validate_recovery_monitoring_outputs](04_validate_recovery_monitoring_outputs.ipynb) | 2 tables, 3 text outputs |
| [05_monitor_fintech_job_runs](05_monitor_fintech_job_runs.ipynb) | 8 tables |

## Saved SQL queries

These queries retain their `.sql` extension. Their execution results are not included in the notebook-only HTML export.

- [01_validate_customers_staging_sql](01_validate_customers_staging_sql.sql)
- [02_query_staging_tables_sql](02_query_staging_tables_sql.sql)

## Installment ID conflict test

This test adds a second row with the same `installment_id` and a different
`payment_status`. The uniqueness check must stop the curated table write.

Insert the block below immediately before `# Persist the joined records.`
in `04_joining_customers_installments`. Run
`fintech_credit_recovery_monitoring_job` with `simulate_duplicate=false`.

```python
# Copy one installment with a different payment status to test whether
# the transformation rejects two rows with the same installment ID.
from pyspark.sql import functions as F

status = F.upper(F.trim(F.col("payment_status")))
test_row = joined_df.limit(1).withColumn(
    "payment_status",
    F.when(status == "PAID", F.lit("PENDING")).otherwise(F.lit("PAID")),
)
joined_df = joined_df.unionByName(test_row)
```

| Task | Observed result |
| --- | --- |
| `transforming_customers_installments` | Failed with `ValueError: Multiple records found for installment IDs [3001].` |
| `refreshing_fintech_dashboard` | Skipped with status **Upstream failed**. |