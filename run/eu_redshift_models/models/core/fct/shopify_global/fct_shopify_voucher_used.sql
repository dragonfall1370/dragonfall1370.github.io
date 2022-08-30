

  create view "airup_eu_dwh"."shopify_global"."fct_shopify_voucher_used__dbt_tmp" as (
    ---Authors: YuShih Hsieh
---Last Modified by: YuShih Hsieh

---###################################################################################################################
        -- this view contains the information on the order_id discount code, type and value
---###################################################################################################################

 

select oe.creation_date, 	
	case when oe.shopify_shop = 'Base' then 'Germany'
	when oe.shopify_shop = 'FR' then 'France'
	when oe.shopify_shop = 'IT' then 'Italy'
	when oe.shopify_shop = 'NL' then 'Netherlands'
	when oe.shopify_shop = 'CH' then 'Switzerland'
	when oe.shopify_shop = 'UK' then 'United Kingdom'
	when  oe.shopify_shop = 'SE' then 'Sweden'
	when  oe.shopify_shop = 'AT' then 'Austria'
	else null
	end as shopify_shop, 
	dae.*, oe.total_discounts, oe.gross_revenue, oe.net_revenue_1, oe.net_revenue_2, net_orders, oe.net_volume
from "airup_eu_dwh"."shopify_global"."fct_discount_application_enriched" dae
left join "airup_eu_dwh"."shopify_global"."fct_order_enriched" oe
on dae.order_id = oe.id 
where oe.total_discounts is not null
order by 1 desc
  ) with no schema binding;
