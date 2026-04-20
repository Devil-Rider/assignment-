--Days of cover per store-SKU on Apr 14, 2026. Days of cover = closing_qty on Apr 14 ÷ average daily units sold over the prior 7 days. Return store_id, sku_id, closing_qty, avg_daily_sales, days_of_cover.

WITH Past_7_Days_Sales AS (
    SELECT 
        store_id,
        sku_id,
        -- Divide by 7.0 to force float division and ensure a true 7-day average
        SUM(units_sold) / 7.0 AS avg_daily_sales 
    FROM db.scratch.order_lines 
    WHERE order_date_ist >= DATEADD(day, -7, '2026-04-14'::date) 
      AND order_date_ist < '2026-04-14'::date
    GROUP BY 1, 2
),
April_14_Inventory AS (
    SELECT 
        store_id,
        sku_id,
        closing_qty
    FROM db.scratch.closing_inv_snapshot
    WHERE snapshot_date = '2026-04-14'::date
)
SELECT 
    inv.store_id,
    inv.sku_id,
    inv.closing_qty,
    COALESCE(s.avg_daily_sales, 0) AS avg_daily_sales,
    -- NULLIF prevents a "division by zero" error. Returns NULL if avg_daily_sales is 0.
    nvl(inv.closing_qty / NULLIF(s.avg_daily_sales, 0),9999) AS days_of_cover
    
FROM April_14_Inventory inv
LEFT JOIN Past_7_Days_Sales s 
    ON inv.store_id = s.store_id 
    AND inv.sku_id = s.sku_id;