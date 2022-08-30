 

with customer_first_orders as 
( -- This cte is to get the customer's first order date and order_ids.
select order_id,
customer_id,
order_date,
first_order_date,
first_order_flag
from (
select id as order_id,
customer_id,
country_grouping, 
country_fullname, 
created_at::date as order_date,
case when created_at = min(created_at) over (partition by customer_id) then 1 else 0 end as first_order_flag,
min(created_at::date) over (partition by customer_id) as first_order_date
from "airup_eu_dwh"."shopify_global"."fct_order_enriched" foe
group by id,
customer_id,
country_grouping, 
country_fullname, 
created_at)
where first_order_flag = 1),

drinking_system_orders as 
( -- This cte is to obtain all the orders which had a drinking system (i.e. SS, Bundle with SS or coloured bottle) as part of the purchase.
SELECT fct_order_line.order_id
FROM "airup_eu_dwh"."shopify_global"."fct_order_enriched" fct_order_enriched
JOIN "airup_eu_dwh"."shopify_global"."fct_order_line" fct_order_line ON fct_order_enriched.id = fct_order_line.order_id
left join "airup_eu_dwh"."shopify_global"."shopify_product_categorisation" shopify_product_categorisation on fct_order_line.sku = shopify_product_categorisation.sku
WHERE category = 'Hardware'),

customer_first_order_drinking_system as 
( -- This cte combines the above 2 ctes to obtain the result set containing customer_ids of the customers whose first purchase was a drinking system in airup.
select customer_id 
from customer_first_orders
join drinking_system_orders on customer_first_orders.order_id = drinking_system_orders.order_id)
-- This final query set is to obtain all the historical data (attribute and required metrics) of customers obtained in the previous cte.
select id as order_id,
customer_id,
country_grouping, 
country_fullname, 
created_at::date as order_date,
sum(net_orders) as net_orders,
sum(net_revenue_2) as net_revenue_2
from "airup_eu_dwh"."shopify_global"."fct_order_enriched" foe
where foe.customer_id in (select customer_id from customer_first_order_drinking_system)
group by id,
customer_id,
country_grouping, 
country_fullname, 
created_at::date