--- creator: Nham Dao
--- last modify: Nham Dao
--- summary: view for intuendi sales data




select to_date(concat(concat( cast(s.year as text), case when len(s.period) > 1 then cast(s.period as text)
 else concat('0', cast(s.period as text)) end),'01'),'YYYYMMDD' ) as "date", 
  p.name as product_name,p.sku,  r.name as region_name, s.revenue, s.quantity, s.cost
 from "airup_eu_dwh"."intuendi_sales"."sales" as s
left join "airup_eu_dwh"."intuendi_sales"."products" as p 
on s.product_id = p.id 
left join "airup_eu_dwh"."intuendi_sales"."regions" as r 
on s.region_id = r.id