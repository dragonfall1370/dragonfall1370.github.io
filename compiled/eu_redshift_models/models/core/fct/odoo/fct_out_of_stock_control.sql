--- creator: Nham Dao
--- last modify: Nham Dao
--- summary: out of stock order


with order_enriched as (select id, order_number as order_number_original, 
case 
when order_number = '1534804' then 'DE-1534804'
when STRPOS(order_number, '-' )=0 then 
concat(concat(shipping_address_country_code,'-'),order_number) else order_number end as order_number, 
shipping_address_country_code as country
from "airup_eu_dwh"."shopify_global"."fct_order_enriched"),
stock_picking as 
(select origin, 
CASE
WHEN origin LIKE '%Shop__#%' THEN REPLACE(origin, 'Shop__#', '') --- cannot use ltrim since it strim 'Shop#__#SE-1001' to 'E-1001' while we expect 'SE-1001'
WHEN origin LIKE '%Shop_#%' THEN REPLACE(origin, 'Shop_#', '')
ELSE origin
END AS shopify_order_number,
create_date, 
datediff(hour, create_date::timestamp, current_timestamp::timestamp) as hour_diff,
case when strpos(partner_id, '"')>0 then
	replace(regexp_substr(partner_id , '\"(.+)\"'), '"', '')
	else replace(regexp_substr(partner_id , '\'(.+)\''), '\'', '')  end as partner_id,
    state
from "airup_eu_dwh"."odoo"."stock_picking"
where origin like 'Shop%'), 
partner as 
(select display_name, email from "airup_eu_dwh"."odoo"."res_partner")
select stock_picking.origin,stock_picking.create_date, stock_picking.shopify_order_number, stock_picking.hour_diff, 
stock_picking.state, order_line.sku, order_line.name, order_enriched.country from stock_picking
left join partner
on stock_picking.partner_id = partner.display_name
left join order_enriched
on order_enriched.order_number = stock_picking.shopify_order_number
left join "airup_eu_dwh"."shopify_global"."fct_order_line" order_line
on order_enriched.id = order_line.order_id
where state <> 'done'