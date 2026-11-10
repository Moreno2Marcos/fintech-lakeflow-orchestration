-- Verify credit applications loaded by Lakeflow Jobs.
-- Expected: 6 records.
SELECT *
FROM workspace.fintech_jobs.applications_staging;

-- Verify credit customers loaded by Lakeflow Jobs.
-- Expected: 5 records.
SELECT *
FROM workspace.fintech_jobs.customers_staging;
