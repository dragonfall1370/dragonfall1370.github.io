--- creator: Nham Dao
--- last modify: Nham Dao
--- summary: clean procurement data from stock_picking table


with purchase_order_line_clean as
  (
select
	date_planned,
	partner_id,
	partner_id_number,
	date_order,
	product_id,
	order_id,
	currency_id,
	qty_received,
	qty_ordered,
	price_total,
	price_unit
from
	"airup_eu_dwh"."odoo"."fct_purchase_order_line"),
	--odoo.fct_purchase_order_line),
     product_product_clean as
  (
select
	distinct replace(regexp_substr(categ_id, '\"(.+)\"'), '"', '') as product_cat,
	replace(regexp_substr(product_variant_id, '\"(.+)\"'), '"', '') as product_id
from
	--"airup_eu_dwh"."odoo"."product_product"),
	odoo.product_product),
     partner_country as
  (
select
	distinct split_part(replace(regexp_substr(commercial_partner_id, '\"(.+)\"'), '"', ''), ' ', 1) as partner_id_number,
	replace(regexp_substr(country_id, '\"(.+)\"'), '"', '') as country
from
	"airup_eu_dwh"."odoo"."res_partner" rp), 
	--odoo.res_partner),
   exchange_rate as
   (
select
	conversion_rate_eur as exchanged_rate_usd_eur,
	round(1 / conversion_rate_eur, 4) as exchanged_rate_eur_usd
from
	--"airup_eu_dwh"."odoo_currency"."dim_global_currency_rates"
  odoo_currency.dim_global_currency_rates
where
	creation_date = (
	select
		max(creation_date)
	from
		"airup_eu_dwh"."odoo_currency"."dim_global_currency_rates")
	--odoo_currency.dim_global_currency_rates)
	and currency_abbreviation = 'USD'),
stock_picking_for_procurement as
(
select
	"name",
	origin,
	state,
	product_id,
	product_qty,
	qty_done,
	date_done,
	scheduled_date ,
	create_date
from "airup_eu_dwh"."odoo"."fct_stock_picking_for_procurement"),
summary as (
select
	polc.date_planned,
	polc.partner_id, 
	polc.partner_id_number, 
	polc.date_order, 
	polc.product_id, 
	polc.order_id, 
	polc.currency_id, 
	polc.qty_ordered, 
	polc.qty_received as received_qty_PO,
	polc.price_total, 
	polc.price_unit, 
	spfp.qty_done as actual_received, 
	spfp."name",
	spfp.state,
	spfp.scheduled_date,
	spfp.date_done::TIMESTAMP,
	ppc.product_cat,
	pc.country
from
	purchase_order_line_clean polc
left join product_product_clean ppc on
	polc.product_id = ppc.product_id
left join stock_picking_for_procurement spfp on
	spfp.origin = polc.order_id
	and spfp.product_id = polc.product_id
left join partner_country pc on
	polc.partner_id_number = pc.partner_id_number)
select
	*
from
	summary,
	exchange_rate