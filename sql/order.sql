BEGIN;

CREATE TABLE db.scratch.orders 
DISTSTYLE KEY
DISTKEY (order_id)
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
    order_id,
    order_ts,
    store_id,
    sum(quantity) as units_sold,
    sum(quantity * price) as total_revenue,
    count(*) as sku_count,
    order_ts + INTERVAL '5 hours 30 minutes' AS order_ts_ist, -- assuming the order_ts is in utc.
    date(order_ts + INTERVAL '5 hours 30 minutes') AS order_date_ist,
    getdate() + interval '5 hours 30 minutes' as updated_timestamp

FROM deduped_stores
where row_num = 1
GROUP BY 1,2,3

ALTER TABLE db.scratch.orders ADD PRIMARY KEY (id);

COMMIT;