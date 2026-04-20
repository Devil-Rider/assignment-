
BEGIN;

CREATE or replace TABLE db.scratch.closing_inv_snapshot 
DISTSTYLE KEY
DISTKEY (snapshot_date)
SORTKEY (snapshot_date)
AS

WITH deduped_stores AS (
    SELECT 
        *,
        -- This assigns a 1 to the first record for a store_id, and 2 for the duplicate
        ROW_NUMBER() OVER(PARTITION BY snapshot_date,sku_id,store_id ORDER BY closing_qty) as row_num
    FROM db.scratch.stg_stg_inventory_snapshots
)
SELECT
    str(snapshot_date) || '__' || store_id || '__' || sku_id AS id, -- sku id , sku id is the sku 
    store_id,
    sku_id,
    snapshot_date, 
    closing_qty, 
    getdate() + interval '5 hours 30 minutes' as updated_timestamp
    
FROM deduped_stores
where row_num = 1; -- This drops the duplicate!

-- Step 2: Add the Primary Key constraint
ALTER TABLE db.scratch.closing_inv_snapshot ADD PRIMARY KEY (id);

COMMIT;