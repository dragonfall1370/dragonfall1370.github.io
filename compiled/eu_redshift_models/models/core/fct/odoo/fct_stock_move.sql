--- creator: Nham Dao
--- last modify: Nham Dao
--- summary: clean data from stock_move table



with stock_move as (select
	create_date,
	case when strpos(product_id, '"')>0 then
	replace(regexp_substr(product_id , '\"(.+)\"'), '"', '')
	else replace(regexp_substr(product_id , '\'(.+)\''), '\'', '') end as product_id,
	case when strpos(location_id, '"')>0 then
	replace(regexp_substr(location_id , '\"(.+)\"'), '"', '')
	else replace(regexp_substr(location_id , '\'(.+)\''), '\'', '') end as location_id,
    split_part(case when strpos(picking_type_id, '"')>0 then
	replace(regexp_substr(picking_type_id , '\"(.+)\"'), '"', '')
	else replace(regexp_substr(picking_type_id , '\'(.+)\''), '\'', '') end, ':',2) as picking_type_id,
    product_uom_qty, 
	case when strpos(partner_id, '"')>0 then
	replace(regexp_substr(partner_id , '\"(.+)\"'), '"', '')
	else replace(regexp_substr(partner_id , '\'(.+)\''), '\'', '') end as partner_id,
	origin
from
	"airup_eu_dwh"."odoo"."stock_move"), 
sku_clean as (
select create_date, product_id, replace(replace(split_part(product_id, ' ', 1),'[',''),']','') as sku, 
trim(regexp_replace(product_id, '\\[[^]]*]')) as product_name, location_id, picking_type_id, product_uom_qty, origin, partner_id
from stock_move)
select create_date, product_id, sku, 
case when product_name like '%]%' then trim(split_part(product_name,']', 2))
else product_name end as product_name
, location_id, picking_type_id, product_uom_qty, origin, partner_id
from sku_clean