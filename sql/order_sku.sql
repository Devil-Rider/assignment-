BEGIN;

CREATE TABLE DB.scratch.order_lines 
DISTSTYLE KEY
DISTKEY (order_id) -- Distributing by order_id ensures it joins incredibly fast with orders
SORTKEY (order_date_ist)
AS
WITH deduped_stores AS (
    SELECT 
        *,
        -- This assigns a 1 to the first record for a store_id, and 2 for the duplicate
        ROW_NUMBER() OVER(PARTITION BY store_id,sku_id ORDER BY order_ts desc) as row_num
    FROM db.scratch.stg_orders
)
SELECT 
    -- This combines the two columns with an 'x' in the middle
    order_id || '__' || sku_id AS id, 
    order_id,
    sku_id,
    quantity,
    unit_price,
    promo,
    date(order_ts + INTERVAL '5 hours 30 minutes') AS order_date_ist
FROM deduped_stores;

ALTER TABLE db.scratch.dim_store ADD PRIMARY KEY (id);

COMMIT;