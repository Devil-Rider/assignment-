--Revenue by city and category for the two-week window, sorted by revenue descending. Treat different casings of the same category as the same category.

select 
st.city,
lower(sk.category) as category,
sum(a_sku.quantity * a_sku.price) as revenue




from db.scratch.order as a  -- only order level data , like store_id , date & time , total value , etc 
left join db.scratch.order_sku as a_sku -- only order level data , like sku_id , sku qty  , sku level value , etc 
    on a.id = a_sku.order_id
left join db.scratch.dim_sku as sk  -- sku related  information 
    on sk.sku_id = a_sku.sku_id
left join db.scratch.dim_store as st -- store related  information 
    on st.store_id = a.store_id

group by all 
order by 3 desc 