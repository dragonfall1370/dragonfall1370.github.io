

with
    -- ################################
    -- gathering all orders enriched by campaign information
    -- flagging customers who purchased via campaign
    -- ################################
	prep as (
         select
               order_enriched.customer_id,
               order_enriched.created_at,
               order_enriched.id as order_id,
               order_enriched.country_abbreviation as country,
               shopify_product_categorisation.category,
               shopify_product_categorisation.subcategory_1,
               shopify_product_categorisation.subcategory_3,
               min(created_at) OVER (PARTITION BY customer_id ORDER BY created_at rows between 1 following and 1 following) AS min_order_date_per_customer,
               max(CASE WHEN campaign_purchase is TRUE then 1 ELSE 0
                   END) OVER (PARTITION BY customer_id) as campaign_customer,
               min(CASE WHEN campaign_purchase is TRUE then created_at
                   END) OVER (PARTITION BY customer_id) as min_campaign_order
         --from dbt_feldm.fct_order_enriched_campaign_enriched_offline order_enriched
         from "airup_eu_dwh"."reports"."fct_order_enriched_campaign_enriched_offline" order_enriched
            left join shopify_global.fct_order_line
                on order_enriched.id = fct_order_line.order_id
            left join shopify_global.shopify_product_categorisation
                on fct_order_line.sku = shopify_product_categorisation.sku
--        where
--             created_at > '2021-10-01' -- reducing the amount of data (campaign was launched after 10/2021)
        ),

    -- ################################
    -- getting only  a subset of data (customers with campaign)
    -- ################################
    campaign_orders as
        (select
                customer_id,
                order_id,
                created_at,
                country,
                case
                    when category = 'Hardware' then 'Starter set'
                    when category = 'Accessories' then 'Accessories'
                    when category = 'Flavour' then subcategory_3
                    when subcategory_3 is null then 'Unmapped Product' end
                as flavour,
                dense_rank() OVER (PARTITION BY customer_id ORDER BY created_at) AS nth_order
         from prep
         where
                campaign_customer = 1
                and
                created_at >= min_campaign_order
        ),

     campaing_orders_prep as (
         select
            customer_id,
            order_id,
            flavour
         from
            campaign_orders
         where
            nth_order = 1
         group by
            1,2,3
     ),

     number_of_first_orders as (
     select count(distinct order_id) as order_count from campaing_orders_prep
     ),

     final as (
         select
            flavour,
            order_count,
            count(*) / order_count::float as share
         from campaing_orders_prep
            left join number_of_first_orders
                on 1 = 1
         group by 1,2
     )
select
    *
from
    final