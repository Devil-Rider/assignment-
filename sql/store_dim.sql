BEGIN;

-- Step 1: Rebuild the table
CREATE OR REPLACE TABLE db.scratch.dim_store 
DISTSTYLE ALL
SORTKEY (store_id)
AS
WITH deduped_stores AS (
    SELECT 
        *,
        ROW_NUMBER() OVER(PARTITION BY store_id ORDER BY store_id) as row_num
    FROM db.scratch.stg_store_master
)
SELECT 
    id, 
    name,
    city,
    region,
    launched_on,
    getdate() + interval '5 hours 30 minutes' as updated_timestamp
FROM deduped_stores
WHERE row_num = 1;

-- Step 2: Re-apply the Primary Key
ALTER TABLE db.scratch.dim_store ADD PRIMARY KEY (id);

COMMIT;