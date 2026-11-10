
-- Validate record counts and data consistency.
WITH validation AS (
    SELECT
        -- Count source records.
        (SELECT COUNT(*)
         FROM workspace.fintech_jobs.credit_customers)
         AS source_count,

        -- Count staging records.
        (SELECT COUNT(*)
         FROM workspace.fintech_jobs.customers_staging)
         AS target_count,

        -- Find source records missing from staging.
        (SELECT COUNT(*) FROM (
            SELECT *
            FROM workspace.fintech_jobs.credit_customers
            EXCEPT ALL
            SELECT *
            FROM workspace.fintech_jobs.customers_staging
        )) AS missing_records,

        -- Find unexpected records in staging.
        (SELECT COUNT(*) FROM (
            SELECT *
            FROM workspace.fintech_jobs.customers_staging
            EXCEPT ALL
            SELECT *
            FROM workspace.fintech_jobs.credit_customers
        )) AS unexpected_records
)

SELECT *,
    CASE
        WHEN source_count = 5
         AND target_count = 5
         AND missing_records = 0
         AND unexpected_records = 0
        THEN 'PASS'
        ELSE 'FAIL'
    END AS validation_status
FROM validation;
