--- creator: Nham Dao
--- last modify: Nham Dao
--- summary: view with purchase order information (including country and information from stock_picking table)



with purchase_order_line_clean as
  (
select
	date_planned,
	partner_id,
	partner_id_number,
	split_part(product_id, ' ',1) as product_id_number,
	date_order,
	product_id,
	order_id,
	currency_id,
	qty_received,
	qty_ordered,
	price_total,
	price_unit,
	state
from
	"airup_eu_dwh"."odoo"."fct_purchase_order_line"),
     product_product_clean as
  (
select
	distinct case when strpos(categ_id, '"')>0 then 
    replace(regexp_substr(categ_id, '\"(.+)\"'), '"', '')
    else
    replace(regexp_substr(categ_id, '\'(.+)\''), '\'', '') end as product_cat,
    case when strpos(product_variant_id, '"')>0 then 
    replace(regexp_substr(product_variant_id, '\"(.+)\"'), '"', '')
    else
	replace(regexp_substr(product_variant_id, '\'(.+)\''), '\'', '') end as product_id,
	case when strpos(product_variant_id, '"')>0 then 
    split_part(replace(regexp_substr(product_variant_id, '\"(.+)\"'), '"', ''), ' ', 1)
    else split_part(replace(regexp_substr(product_variant_id, '\'(.+)\''), '\'', ''), ' ', 1) end as product_id_number
from
	"airup_eu_dwh"."odoo"."product_product"
	where lower(default_code) <> 'false'),
     partner_country as --1 partner may have multiple countries in the table, here we take the latest one
  (select partner_id_number, country from 
(select
	distinct case when strpos(commercial_partner_id, '"')>0 then 
    split_part(replace(regexp_substr(commercial_partner_id, '\"(.+)\"'), '"', ''), ' ', 1)
    else split_part(replace(regexp_substr(commercial_partner_id, '\'(.+)\''), '\'', ''), ' ', 1) end as partner_id_number,
	case when strpos(commercial_partner_id, '"')>0 then 
    replace(regexp_substr(country_id, '\"(.+)\"'), '"', '') 
    else replace(regexp_substr(country_id, '\'(.+)\''), '\'', '') end as country,
	row_number () over(partition by partner_id_number order by "_fivetran_synced" desc) as rank_index
from
	"airup_eu_dwh"."odoo"."res_partner" rp)
	where rank_index = 1), 
   exchange_rate as
   (
select
	conversion_rate_eur as exchanged_rate_usd_eur,
	round(1 / conversion_rate_eur, 4) as exchanged_rate_eur_usd
from
	"airup_eu_dwh"."odoo_currency"."dim_global_currency_rates"
where
	creation_date = (
	select
		max(creation_date)
	from
		"airup_eu_dwh"."odoo_currency"."dim_global_currency_rates")
	and currency_abbreviation = 'USD'),

stock_picking_for_procurement as
-- 1 origin (PO number) can include more than 1 names (Geodis receipt). This causes doublicated rows when joining purchase order line with stock move line => only take Geodis receipt of latest scheduled date
(
select
	"name",
	origin,
	state,
	product_id,
	product_id_number,
	product_qty,
	date_done,
	scheduled_date ,
	create_date
from
	(
	select
		"name",
		origin,
		state,
		product_id,
		split_part(product_id, ' ',1) as product_id_number, 
		date_done,
		product_qty,
		scheduled_date ,
		create_date,
		row_number() over(partition by origin,
		product_id
	order by
		scheduled_date desc) as rank_order
		--does not include "name" since for 1 product_id we just want to have 1 "name" for it
	from
		"airup_eu_dwh"."odoo"."fct_stock_picking_for_procurement") t1
where
	rank_order = 1),
summary as (
select
	polc.*,
	spfp."name",
	spfp.scheduled_date,
	spfp.date_done::TIMESTAMP,
	ppc.product_cat,
	pc.country
from
	purchase_order_line_clean polc
left join product_product_clean ppc on
	polc.product_id_number = ppc.product_id_number
left join stock_picking_for_procurement spfp on
	spfp.origin = polc.order_id
	and spfp.product_id_number = polc.product_id_number
left join partner_country pc on
	polc.partner_id_number = pc.partner_id_number)
select
	*
from
	summary,
	exchange_rate