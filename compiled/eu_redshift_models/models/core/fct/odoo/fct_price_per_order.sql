--created by: Nham Dao

--This view shows the price of order based on eta


with purchase_order_received_qty as 
(
select
	*
from
	"airup_eu_dwh"."odoo"."fct_purchase_order_received_qty"),
purchase_unit_price as (
select
	*,
	coalesce(date_start, '2019-01-01') as new_date_start,
	coalesce(date_end, dateadd('year', 3, current_date)::date) as new_date_end
from
	"airup_eu_dwh"."odoo"."fct_purchase_unit_price")
select
	purchase_order_received_qty.*,
	purchase_unit_price.price,
	purchase_unit_price.exchanged_rate_usd_eur
,
	purchase_unit_price.exchanged_rate_eur_usd
from
	purchase_order_received_qty
left join purchase_unit_price
on
	purchase_order_received_qty.partner_id = purchase_unit_price."name"
	and purchase_order_received_qty.product_id = purchase_unit_price.product_id
	and purchase_order_received_qty.eta_month between new_date_start and new_date_end