--For each store, the top 3 SKUs by units sold. Handle ties however you prefer — just be explicit about your choice.


WITH SKU_Sales AS (
    SELECT 
        store_id,
        sku_id,
        SUM(quantity) AS total_units

    FROM db.scratch.order_lines
    GROUP BY 1, 2
),
Ranked_SKUs AS (
    SELECT 
        store_id,
        sku_id,
        total_units,
        -- DENSE_RANK handles ties without skipping subsequent rank numbers
        DENSE_RANK() OVER(PARTITION BY store_id ORDER BY total_units DESC) as sales_rank
    FROM SKU_Sales
)
SELECT 
    store_id,
    sku_id,
    total_units,
    sales_rank
FROM Ranked_SKUs
WHERE sales_rank <= 3
ORDER BY store_id, sales_rank;