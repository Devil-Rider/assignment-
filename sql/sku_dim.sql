BEGIN;

CREATE or replace TABLE db.scratch.dim_sku
DISTSTYLE ALL
SORTKEY (sku_id)
AS

WITH deduped_stores AS (
    SELECT 
        *,
        -- This assigns a 1 to the first record for a store_id, and 2 for the duplicate
        ROW_NUMBER() OVER(PARTITION BY sku_id ORDER BY sku_id) as row_num
    FROM db.scratch.stg_sku_master
)
SELECT
    id, -- sku id , sku id is the sku 
    name,	-- sku name
    category, -- category of sku 
    mrp, -- mrp of sku ( will keep on updating as the csv is updated)
    pack_size -- pack size of sku
    getdate() + interval '5 hours 30 minutes' as updated_timestamp
    
FROM deduped_stores
where row_num = 1; -- This drops the duplicate!

-- Step 2: Add the Primary Key constraint
ALTER TABLE db.scratch.dim_sku ADD PRIMARY KEY (id);

COMMIT;