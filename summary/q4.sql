
--A simple daily trend: total orders, total units, and total revenue per day across the full window. Flag any day where revenue dropped more than 20% day-over-day.


WITH Daily_Aggregates AS (
    SELECT 
        order_date_ist as sales_date,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(quantity) AS total_units,
        SUM(quantity * price) AS total_revenue

    FROM db.scratch.order_lines
    GROUP BY 1
),
Trend_With_Lag AS (
    SELECT 
        sales_date,
        total_orders,
        total_units,
        total_revenue,
        -- Grab the revenue from the sequentially previous sales date
        LAG(total_revenue) OVER(ORDER BY sales_date) AS prev_day_revenue

    FROM Daily_Aggregates
)
SELECT 
    sales_date,
    total_orders,
    total_units,
    total_revenue,
    prev_day_revenue,
    CASE 
        -- Check if previous day had revenue, then check if the drop is > 20%
        WHEN nvl(prev_day_revenue,0) > 0 
         AND (prev_day_revenue - total_revenue) / prev_day_revenue > 0.20 
        THEN 1 
        ELSE 0 
    END AS is_20_pct_drop_flag
    
FROM Trend_With_Lag
ORDER BY sales_date;