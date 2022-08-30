--- creator: Nham Dao
--- last modify: Nham Dao
--- summary: daily odoo inventory data


select replace(replace(split_part(replace(regexp_substr(product_id, '\'(.+)\''), '\'', ''),' ',1), '[', ''), ']','') as sku 
, trim(regexp_replace(display_name, '\\[[^]]*]')) as product_name
, null as warehouse
, replace(regexp_substr(location_id, '\'(.+)\''), '\'', '') as location 
, replace(regexp_substr(lot_id, '\'(.+)\''), '\'', '') as lot
, sum(inventory_quantity) quantity_on_hand 
, sum(reserved_quantity) reserved_quantity
, sum(value) value
, glue_timestamp::date as "timestamp"
from "airup_eu_dwh"."odoo"."stock_quant" sq 
where glue_timestamp::date >= '2022-06-01'
group by 1,2,3,4,5,9