--created by: Nham Dao

--This view show the order and received qty of purchase order in Geodis warehouse



with order_date_order_qty as 
(
select
	date_trunc('month', date_order)::date as order_month,
	partner_id,
	product_id,
	sum(qty_ordered) as order_qty
from
    "airup_eu_dwh"."odoo"."fct_purchase_order_enriched" fpoe
where
	name is not null
group by
	1,
	2,
	3),
stock_picking_with_supplier as
--1 name can have multiple origin -> should not add quantity here
(
select
	spfp.name,
	spfp.product_id,
	date_trunc('month', spfp.date_done)::date as ETA_month,
	case
		when pol.partner_id = '[964177] Qmax Asia Manufacturing Ltd. attn. Whitney Feng'
then '[964177] Qmax Asia Manufacturing Ltd.'
		else pol.partner_id
	end as partner_id
from
	"airup_eu_dwh"."odoo"."fct_stock_picking_for_procurement" spfp
left join odoo.fct_purchase_order_line pol 
on
	spfp.origin = pol.order_id
where
	(origin like 'PO%'
		and not origin like '%Return%')
group by
	1,
	2,
	3,
	4),
stock_move_line as 
(
select
	reference,
	product_id,
	sum(qty_done) as received_qty
from "airup_eu_dwh"."odoo"."fct_stock_move_line" fsml
group by
	1,
	2),
schedule_date_received_qty as 
(
select
	stock_move_line.product_id,
	stock_picking_with_supplier.ETA_month,
	stock_picking_with_supplier.partner_id,
	sum(stock_move_line.received_qty) as received_qty
from
	stock_picking_with_supplier
left join stock_move_line 
on
	stock_move_line.reference = stock_picking_with_supplier.name
	and split_part(stock_move_line.product_id, ' ', 1) = split_part(stock_picking_with_supplier.product_id, ' ', 1)
group by
	1,
	2,
	3),
product_category as 
(select
	distinct case when strpos(categ_id, '"')>0 then 
    replace(regexp_substr(categ_id, '\"(.+)\"'), '"', '')
    else
    replace(regexp_substr(categ_id, '\'(.+)\''), '\'', '') end as product_cat,
    case when strpos(product_variant_id, '"')>0 then 
    replace(regexp_substr(product_variant_id, '\"(.+)\"'), '"', '')
    else
	replace(regexp_substr(product_variant_id, '\'(.+)\''), '\'', '') end as product_id
from
	"airup_eu_dwh"."odoo"."product_product"
	where lower(default_code) <> 'false'),
summary as 
(select
	order_date_order_qty.partner_id,
	order_date_order_qty.product_id,
	order_date_order_qty.order_month,
	order_date_order_qty.order_qty,
	product_category.product_cat,
	schedule_date_received_qty.ETA_month,
	schedule_date_received_qty.received_qty
from
	order_date_order_qty
left join schedule_date_received_qty
on
	split_part(order_date_order_qty.partner_id,' ',1) = split_part(schedule_date_received_qty.partner_id, ' ', 1)
	and split_part(order_date_order_qty.product_id, ' ', 1) = split_part(schedule_date_received_qty.product_id, ' ', 1)
left join product_category
on split_part(order_date_order_qty.product_id, ' ',1) = split_part(product_category.product_id, ' ', 1)), 
purchase_unit_price as (
select
	"name", product_id, price,currency,exchanged_rate_usd_eur,exchanged_rate_eur_usd,
	new_date_start,
	new_date_end
from
	"airup_eu_dwh"."odoo"."fct_purchase_unit_price"
	group by 1,2,3,4,5,6,7,8)
select
	summary.*,
	purchase_unit_price.price,
	purchase_unit_price.currency,
	purchase_unit_price.exchanged_rate_usd_eur
,
	purchase_unit_price.exchanged_rate_eur_usd
from
	summary
left join purchase_unit_price
on
	split_part(summary.partner_id, ' ', 1) = split_part(purchase_unit_price."name", ' ', 1)
	and split_part(summary.product_id, ' ', 1) = split_part(purchase_unit_price.product_id, ' ', 1)
	and summary.eta_month between new_date_start and new_date_end