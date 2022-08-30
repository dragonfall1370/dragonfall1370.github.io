---- Author: Abhishek Pathak
---- Created: 15/02/2022
---- Modified: 14/06/2022

 
---##########################################################################################################################################################

        ---calculate all the pods and ss sold across channels i.e D2C, BOL, AMZ & Offline. The option for including or excluding pod count in SS is feasible
        ---and SS bundles with multiple SS are accounted for.

---##########################################################################################################################################################
with d2c_pods_ss_excl_ss_pods as ( /* Calculating pods and ss for D2C channel which does not include pods in starter sets. */
select
	order_enriched.customer_id,
	order_enriched.country_fullname as country,
	order_enriched.created_at as order_timestamp,
	order_enriched.created_at::date as order_date,
	-- product_enriched.product_type,
	case
		when shopify_product_categorisation.category::text = 'Flavour'::text then order_line.quantity * shopify_product_categorisation.pods_per_flavour_pouch
		else 0::double precision
	end as d2c_pods,
	case
		when shopify_product_categorisation.category::text = 'Hardware'::text then order_line.quantity
		when shopify_product_categorisation.sku in ('140000032','140000033','140000034','140000035','140000036','140000051','140000059','140000043','140000044') then order_line.quantity * 2
		else 0::double precision
	end as d2c_ss
from
	"airup_eu_dwh"."shopify_global"."fct_order_enriched" order_enriched
left join "airup_eu_dwh"."shopify_global"."fct_order_line" order_line on
	order_enriched.id = order_line.order_id
-- left join "airup_eu_dwh"."shopify_global"."dim_product_enriched" product_enriched on
-- 	order_line.product_id = product_enriched.id
left join "airup_eu_dwh"."shopify_global"."shopify_product_categorisation" shopify_product_categorisation  on
	order_line.sku::text = shopify_product_categorisation.sku::text
where
	order_enriched.financial_status in ('paid','partially_refunded')
      ),
agg_d2c_pods_ss_1 as (/* final pass for D2C channel to aggregate/group the pods and ss at required granularity */
select
	order_date,
	country,
	'd2c'::text as purchase_channel,
	sum(coalesce(d2c_ss, 0)) as total_ss,
	sum(coalesce(d2c_pods, 0)) / 3 as total_pods
from
	d2c_pods_ss_excl_ss_pods
group by
	order_date,
	country
      ),
amz_pods_ss_excl_ss_pods as (/* Calculating pods and ss for AMZ channel which does not include pods in starter sets. */
select
	orders_fulfilled_shipments_manual_upload_enriched.amazon_order_item_id,
	orders_fulfilled_shipments_manual_upload_enriched.country_fullname as country,
	orders_fulfilled_shipments_manual_upload_enriched.purchase_date::date as order_date,
	orders_fulfilled_shipments_manual_upload_enriched.product_type,
	case
		when custom_pod_flavour_mapping.product_type = 'Pods'::text then (orders_fulfilled_shipments_manual_upload_enriched.quantity_shipped * custom_pod_flavour_mapping.pods_per_flavour_pouch)
		else 0::double precision
	end as amz_pods,
	case
		when custom_pod_flavour_mapping.product_type = 'Starter Set'::text then orders_fulfilled_shipments_manual_upload_enriched.quantity_shipped::double precision
		else 0::double precision
	end as amz_ss
from
	"airup_eu_dwh"."amazon"."fct_orders_fulfilled_shipments_enriched" orders_fulfilled_shipments_manual_upload_enriched
left join "airup_eu_dwh"."amazon"."custom_pod_flavour_mapping" custom_pod_flavour_mapping on
	orders_fulfilled_shipments_manual_upload_enriched.product_name = custom_pod_flavour_mapping.product_name
        ),
agg_amz_pods_ss_1 as (/* final pass for AMZ channel to aggregate/group the pods and ss at required granularity */
select
	order_date,
	country,
	'amazon'::text as purchase_channel,
	sum(coalesce(amz_ss, 0::double precision)) as total_ss,
	sum(coalesce(amz_pods, 0::double precision)) / 3 as total_pods
from
	amz_pods_ss_excl_ss_pods
group by
	order_date,
	country
    ),
bol_pods_ss_excl_ss_pods as (/* Calculating pods and ss for BOL channel which does not include pods in starter sets. */
select
	order_enriched.id as order_id,
	order_enriched.country_fullname as country,
	order_enriched.created_at as order_timestamp,
	order_enriched.created_at::date as order_date,
	product_enriched.product_type,
	case
		when lower(product_enriched.product_type) in ('pods','aromapod','aromapod-bundle','aromapod-bundle-mix') then order_line.quantity
		else 0::double precision
	end as bol_pods,
	case
		when lower(product_enriched.product_type) in ('starter kit'::text,'starter-set') then order_line.quantity
		else 0::double precision
	end as bol_ss
from
	"airup_eu_dwh"."shopify_marketplace"."fct_order_enriched_marketplace" order_enriched
left join "airup_eu_dwh"."shopify_marketplace"."fct_order_line_marketplace" order_line on
	order_enriched.id = order_line.order_id
left join "airup_eu_dwh"."shopify_marketplace"."seed_marketplace_product_enriched" product_enriched on
	order_line.product_id = product_enriched.id
where
	order_enriched.cancelled_at is null
	and (order_enriched.financial_status in ('paid','partially_refunded'))
    ),
agg_bol_pods_ss_1 as (/* final pass for BOL channel to aggregate/group the pods and ss at required granularity */
select
	order_date,
	country,
	'BOL'::text as purchase_channel,
	sum(coalesce(bol_ss, 0::double precision)) as total_ss,
	sum(coalesce(bol_pods, 0::double precision)) as total_pods
from
	bol_pods_ss_excl_ss_pods
group by
	order_date,
	country
      ),
-- agg_offline_pods_ss as ( /* This pass is commented out for now because automated offline sales from Odoo is incorrect. */
-- select
-- 	offline_sales.order_line_create_date::date as order_date,
-- 	offline_sales.country_name as country,
-- 	'Offline Sales'::text as purchase_channel,
-- 	sum(
--                 case
--                     when offline_sales.sku_pu4_8::text = '100000030'::text then offline_sales.qty_delivered
--                     else 0::double precision
--                 end) as total_ss,
-- 	sum(
--                 case
--                     when offline_sales.sku_pu4_8::text not in ('100000030', '130000007') then offline_sales.qty_delivered
--                     else 0::double precision
--                 end) as total_pods
-- from
-- 	odoo.offline_sales
-- group by
-- 	(offline_sales.order_line_create_date::date),
-- 	offline_sales.country_name
--         ),
agg_offline_excel_pods_ss_1 as (/* Calculating pods and ss for Offline channel which does not include pods in starter sets.. The numbers are pods and ss are taken from manual excel file maintained by Inken's team */
select
	offline_sales_excel.date as order_date,
	'Germany'::text as country,
	'Offline'::text as purchase_channel,
	sum(offline_sales_excel.pods) as total_pods,
	sum(offline_sales_excel.starter_sets) as total_ss
from
	"airup_eu_dwh"."odoo"."seed_offline_sales_excel" offline_sales_excel
group by
	offline_sales_excel.date
       ),
d2c_pods_ss_incl_ss_pods as ( /* Calculating pods and ss for D2C channel which includes pods in starter sets. */
select
	order_enriched.customer_id,
	order_enriched.country_fullname as country,
	order_enriched.created_at as order_timestamp,
	order_enriched.created_at::date as order_date,
	case
		when shopify_product_categorisation.category::text = 'Flavour'::text then coalesce(order_line.quantity, 0) *  coalesce(shopify_product_categorisation.pods_per_flavour_pouch, 0)
		when shopify_product_categorisation.category::text = 'Hardware'::text then coalesce(order_line.quantity, 0) *  coalesce(shopify_product_categorisation.pods_per_flavour_pouch, 0)
		else 0::double precision
	end as d2c_pods,
	case
		when shopify_product_categorisation.category::text = 'Hardware'::text then order_line.quantity
		when shopify_product_categorisation.sku in ('140000032','140000033','140000034','140000035','140000036','140000051','140000059','140000043','140000044') then order_line.quantity * 2
		else 0::double precision
	end as d2c_ss
from
	"airup_eu_dwh"."shopify_global"."fct_order_enriched" order_enriched
left join "airup_eu_dwh"."shopify_global"."fct_order_line" order_line on
	order_enriched.id = order_line.order_id
left join "airup_eu_dwh"."shopify_global"."shopify_product_categorisation" shopify_product_categorisation  on
	order_line.sku::text = shopify_product_categorisation.sku::text
where
	order_enriched.financial_status in ('paid','partially_refunded')
      ),
agg_d2c_pods_ss_2 as (/* final pass for D2C channel to aggregate/group the pods and ss at required granularity */
select
	order_date,
	country,
	'd2c'::text as purchase_channel,
	sum(coalesce(d2c_ss, 0)) as total_ss,
	sum(coalesce(d2c_pods, 0)) / 3 as total_pods
from
	d2c_pods_ss_incl_ss_pods
group by
	order_date,
	country
      ),
amz_pods_ss_incl_ss_pods as (/* Calculating pods and ss for AMZ channel which includes pods in starter sets. */
select
	orders_fulfilled_shipments_manual_upload_enriched.amazon_order_item_id,
	orders_fulfilled_shipments_manual_upload_enriched.country_fullname as country,
	orders_fulfilled_shipments_manual_upload_enriched.purchase_date::date as order_date,
	orders_fulfilled_shipments_manual_upload_enriched.product_type,
	case
		when custom_pod_flavour_mapping.product_type = 'Pods'::text then (coalesce(orders_fulfilled_shipments_manual_upload_enriched.quantity_shipped, 0) * coalesce(custom_pod_flavour_mapping.pods_per_flavour_pouch, 0))
		when custom_pod_flavour_mapping.product_type = 'Starter Set'::text then (coalesce(orders_fulfilled_shipments_manual_upload_enriched.quantity_shipped, 0) * coalesce(custom_pod_flavour_mapping.pods_per_flavour_pouch, 0))
		else 0::double precision
	end as amz_pods,
	case
		when custom_pod_flavour_mapping.product_type = 'Starter Set'::text then orders_fulfilled_shipments_manual_upload_enriched.quantity_shipped::double precision
		else 0::double precision
	end as amz_ss
from
	"airup_eu_dwh"."amazon"."fct_orders_fulfilled_shipments_enriched" orders_fulfilled_shipments_manual_upload_enriched
left join "airup_eu_dwh"."amazon"."custom_pod_flavour_mapping" custom_pod_flavour_mapping on
	orders_fulfilled_shipments_manual_upload_enriched.product_name = custom_pod_flavour_mapping.product_name
        ),
agg_amz_pods_ss_2 as (/* final pass for AMZ channel to aggregate/group the pods and ss at required granularity */
select
	order_date,
	country,
	'amazon'::text as purchase_channel,
	sum(coalesce(amz_ss, 0::double precision)) as total_ss,
	sum(coalesce(amz_pods, 0::double precision)) / 3 as total_pods
from
	amz_pods_ss_incl_ss_pods
group by
	order_date,
	country
    ),
bol_pods_ss_incl_ss_pods as (/* Calculating pods and ss for BOL channel which includes pods in starter sets. */
select
	order_enriched.id as order_id,
	order_enriched.country_fullname as country,
	order_enriched.created_at as order_timestamp,
	order_enriched.created_at::date as order_date,
	product_enriched.product_type,
	case
		when lower(product_enriched.product_type) in ('pods','aromapod','aromapod-bundle','aromapod-bundle-mix') then coalesce(order_line.quantity, 0)*3
		when lower(product_enriched.product_type) in ('Starter Kit') then coalesce(order_line.quantity, 0)*2
		else 0::double precision
	end as bol_pods,
	case
		when lower(product_enriched.product_type) in ('starter kit'::text,'starter-set') then order_line.quantity
		else 0::double precision
	end as bol_ss
from
	"airup_eu_dwh"."shopify_marketplace"."fct_order_enriched_marketplace" order_enriched
left join "airup_eu_dwh"."shopify_marketplace"."fct_order_line_marketplace" order_line on
	order_enriched.id = order_line.order_id
left join "airup_eu_dwh"."shopify_marketplace"."seed_marketplace_product_enriched" product_enriched on
	order_line.product_id = product_enriched.id
where
	order_enriched.cancelled_at is null
	and (order_enriched.financial_status in ('paid','partially_refunded'))
    ),
agg_bol_pods_ss_2 as (/* final pass for BOL channel to aggregate/group the pods and ss at required granularity */
select
	order_date,
	country,
	'BOL'::text as purchase_channel,
	sum(coalesce(bol_ss, 0::double precision)) as total_ss,
	sum(coalesce(bol_pods, 0::double precision)) / 3 as total_pods
from
	bol_pods_ss_incl_ss_pods
group by
	order_date,
	country
      ),
agg_offline_excel_pods_ss_2 as (/* Calculating pods and ss for Offline channel which includes pods in starter sets.. The numbers are pods and ss are taken from manual excel file maintained by Inken's team */
select
	offline_sales_excel.date as order_date,
	'Germany'::text as country,
	'Offline'::text as purchase_channel,
	(sum(offline_sales_excel.pods) + sum(offline_sales_excel.starter_sets::double precision * 2) / 3) as total_pods,
	sum(offline_sales_excel.starter_sets) as total_ss
from
	"airup_eu_dwh"."odoo"."seed_offline_sales_excel" offline_sales_excel
group by
	offline_sales_excel.date
        )
  select /* Unioning the results of all the channels to obtain final results across each channel */
	agg_d2c_pods_ss.order_date,
	agg_d2c_pods_ss.country,
	agg_d2c_pods_ss.purchase_channel,
	'No'::text as ss_pods_included,
	agg_d2c_pods_ss.total_ss,
	agg_d2c_pods_ss.total_pods
from
	agg_d2c_pods_ss_1 agg_d2c_pods_ss
union all
 select
	agg_amz_pods_ss.order_date,
	agg_amz_pods_ss.country,
	agg_amz_pods_ss.purchase_channel,
	'No'::text as ss_pods_included,
	agg_amz_pods_ss.total_ss,
	agg_amz_pods_ss.total_pods
from
	agg_amz_pods_ss_1 agg_amz_pods_ss
union all
 select
	agg_bol_pods_ss.order_date,
	agg_bol_pods_ss.country,
	agg_bol_pods_ss.purchase_channel,
	'No'::text as ss_pods_included,
	agg_bol_pods_ss.total_ss,
	agg_bol_pods_ss.total_pods
from
	agg_bol_pods_ss_1 agg_bol_pods_ss
union all
 select
	agg_offline_excel_pods_ss.order_date,
	agg_offline_excel_pods_ss.country,
	agg_offline_excel_pods_ss.purchase_channel,
	'No'::text as ss_pods_included,
	agg_offline_excel_pods_ss.total_ss,
	agg_offline_excel_pods_ss.total_pods
from
	agg_offline_excel_pods_ss_1 agg_offline_excel_pods_ss
union all
 select /* Unioning the results of all the channels to obtain final results across each channel */
	agg_d2c_pods_ss_2.order_date,
	agg_d2c_pods_ss_2.country,
	agg_d2c_pods_ss_2.purchase_channel,
	'Yes'::text as ss_pods_included,
	agg_d2c_pods_ss_2.total_ss,
	agg_d2c_pods_ss_2.total_pods
from
	agg_d2c_pods_ss_2
union all
 select
	agg_amz_pods_ss_2.order_date,
	agg_amz_pods_ss_2.country,
	agg_amz_pods_ss_2.purchase_channel,
	'Yes'::text as ss_pods_included,
	agg_amz_pods_ss_2.total_ss,
	agg_amz_pods_ss_2.total_pods
from
	agg_amz_pods_ss_2
union all
 select
	agg_bol_pods_ss_2.order_date,
	agg_bol_pods_ss_2.country,
	agg_bol_pods_ss_2.purchase_channel,
	'Yes'::text as ss_pods_included,
	agg_bol_pods_ss_2.total_ss,
	agg_bol_pods_ss_2.total_pods
from
	agg_bol_pods_ss_2
union all
 select
	agg_offline_excel_pods_ss_2.order_date,
	agg_offline_excel_pods_ss_2.country,
	agg_offline_excel_pods_ss_2.purchase_channel,
	'Yes'::text as ss_pods_included,
	agg_offline_excel_pods_ss_2.total_ss,
	agg_offline_excel_pods_ss_2.total_pods
from
	agg_offline_excel_pods_ss_2