

  create view "airup_eu_dwh"."shopify_global"."fct_daily_revenue_by_customer_by_product_original__dbt_tmp" as (
    ---Authors: YuShih Hsieh
---Last Modified by: YuShih Hsieh

---###################################################################################################################
        -- this view contains the information on the order_id discount code, type and value for E-commerce dashboard
---###################################################################################################################

 

with order_pre as (
select ol.creation_date, 
	case when ol.shopify_shop = 'Base' then 'Germany'
	when ol.shopify_shop = 'FR' then 'France'
	when ol.shopify_shop = 'IT' then 'Italy'
	when ol.shopify_shop = 'NL' then 'Netherlands'
	when ol.shopify_shop = 'CH' then 'Switzerland'
	when ol.shopify_shop = 'UK' then 'United Kingdom'
	when  ol.shopify_shop = 'SE' then 'Sweden'
	else null
	end as shopify_shop, 
	ol.id as order_line_id, oe.id as order_id, oe.customer_id, ol.product_id, ol.sku, spc.category as product_category, spc.subcategory as product_subcategory, ol.name as product_name, ol.quantity, olr.quantity as return_quantity,
	ol.price, tl.price as tax, ol.total_discount, olr.subtotal as return_price, olr.total_tax as return_tax, (olr.subtotal - olr.total_tax) as net_return_price 
from "airup_eu_dwh"."shopify_global"."fct_order_line" ol
left join "airup_eu_dwh"."shopify_global"."fct_tax_line" tl
on ol.id = tl.order_line_id 
and ol.creation_date = tl.creation_date 
left join "airup_eu_dwh"."shopify_global"."fct_order_line_refund" olr
on ol.id = olr.order_line_id
left join "airup_eu_dwh"."shopify_global"."dim_ecommerce_product_category" spc
on ol.sku = spc.sku
left join "airup_eu_dwh"."shopify_global"."fct_order_enriched" oe 
on ol.order_id = oe.id 
)
	select creation_date, shopify_shop, order_line_id, order_id, customer_id, product_id, sku, product_category, product_subcategory, product_name, quantity, return_quantity, 
	price * quantity as gross_revenue, 
	price * quantity + coalesce(total_discount) - coalesce(tax, 0) - coalesce(net_return_price, 0) as net_reveune_1,
	price * quantity - coalesce(tax, 0) - coalesce(net_return_price, 0) as net_revenue_2
	from order_pre
	order by creation_date desc, order_line_id
  ) with no schema binding;
