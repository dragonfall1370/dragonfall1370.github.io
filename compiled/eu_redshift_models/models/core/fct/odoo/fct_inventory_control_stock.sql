--- creator: Nham Dao
--- last modify: Nham Dao
--- summary: table to calculate the inventory reach and classify product id into out-of-stock order


--sales data
with average_sales_data as
(
select
	sku,
	product_name,
	location_id,
	country,
	round(sum(product_uom_qty)/ count(distinct(create_date::date))) as average_sales
from
	"airup_eu_dwh"."odoo"."fct_stock_move" stock_move
left join "airup_eu_dwh"."odoo"."seed_whs_country_mapping" mapping
on
	stock_move.location_id = mapping.warehouse_odoo
	--extract only data from our warehouse and for product_demand purpose
where
	location_id in ('DCUK1/Stock', 'GEOAP/Stock', 'DEDE1/Stock', 'DCFR1/Stock' )
	and lower(trim(picking_type_id)) = 'delivery orders'
	and create_date::date between current_date -30 and current_date-1
	and lower(origin) <> 'false' 
group by
	1,
	2,
	3,
	4), 
-- inventory data
inventory_data as 
(
select
	sku,
	location as location_id,
	min(product_name) as product_name, --in case of typo error 1 sku has 2 product name
	sum(quantity_on_hand - reserved_quantity) as inventory_qty
from
	"airup_eu_dwh"."odoo"."fct_odoo_inventory"
where
	location in ('DCUK1/Stock', 'GEOAP/Stock', 'DEDE1/Stock', 'DCFR1/Stock' )
		and timestamp = current_date
	group by
		1,
		2), 

--get date and time when inventory data was synced
max_inventory_date as 
(
select
	max(glue_timestamp) as inventory_update_time
from
	"airup_eu_dwh"."odoo"."stock_quant"),
active_sku as 
(select
	sku.sku,
	sku.warehouse,
	min(sku.filter) as filter,
	mapping.warehouse_odoo as location_id
from
	"airup_eu_dwh"."odoo"."active_sku_manual" sku
left join "airup_eu_dwh"."odoo"."seed_whs_country_mapping" mapping
on
	sku.warehouse = mapping.warehouse
group by 1,2,4
),
next_inbound_clean as (
select
	to_date(scheduled_date, 'dd/mm/yyyy', false) as scheduled_date,
	sku,
	next_inbound.warehouse,
	mapping.warehouse_odoo as location_id
from
	"airup_eu_dwh"."odoo"."next_inbound_date_manual" next_inbound
left join "airup_eu_dwh"."odoo"."seed_whs_country_mapping" mapping
on
	next_inbound.warehouse = mapping.warehouse
where lower(next_inbound.grouped_status) = 'open'
),
latest_eta as (
	select sku, location_id, min(scheduled_date) as scheduled_date
	from next_inbound_clean
	group by 1,2
),
inventory_enriched as
(select
	case when sale.sku is not null then sale.sku else inv.sku end as sku,
	sale.sku as sale_sku, 
	inv.sku as inventory_sku,
	case when sale.product_name is not null then sale.product_name else inv.product_name end as product_name,
	case when sale.location_id is not null then sale.location_id else inv.location_id end as location_id,
	sale.country,
	sale.average_sales,
	inv.inventory_qty,
	active_sku.filter,
	case
		when average_sales != 0 and inventory_qty!= 0 then round(inventory_qty / average_sales)
		when average_sales != 0 and (inventory_qty = 0 or inventory_qty is null) then 0
	end as inv_reach,
	max_inventory_date.inventory_update_time,
	eta.scheduled_date
from
	average_sales_data sale
full join inventory_data inv 
on sale.location_id = inv.location_id 
and sale.sku = inv.sku
left join latest_eta eta 
on eta.sku = sale.sku 
and eta.location_id = sale.location_id
left join active_sku 
on active_sku.sku = sale.sku 
and active_sku.location_id = sale.location_id
cross join max_inventory_date) 
select *, 
case when inv_reach <5 then 'OOS'
when inv_reach between 5 and 30 then 'HIGH'
when inv_reach between 30 and 60 then 'MEDIUM'
when inv_reach >60 then 'LOW'
end
as risk
from inventory_enriched