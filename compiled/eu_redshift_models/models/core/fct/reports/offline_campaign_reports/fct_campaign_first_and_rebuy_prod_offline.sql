

with
    -- ################################
    -- gathering all orders enriched by campaign information
    -- flagging customers who purchased via certain voucher campaign
    -- ################################
    prep as (
         select
               order_enriched.customer_id,
               order_enriched.created_at,
               order_enriched.id as order_number,
               order_enriched.country_abbreviation as country,
               shopify_product_categorisation.category,
               shopify_product_categorisation.subcategory_1,
               shopify_product_categorisation.subcategory_3,
               shopify_product_categorisation.pods_per_flavour_pouch as pods_per_sku,
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
 		where
             created_at > '2021-10-01' -- reducing the amount of data (campaign was launched after 10/2021) / alternatively add CTE to identify first relevant date
        ),

    -- ################################
    -- getting only  a subset of data (customers with campaign)
    -- ################################
    campaign_orders as
        (select customer_id,
                order_number,
                created_at,
                country,
                case
                    when category = 'Hardware' then 'Starter set'
                    when category = 'Accessories' then 'Accessories'
                    when category = 'Flavour' then subcategory_3
                    when subcategory_3 is null then 'Unmapped Product' end
                as flavour,
                dense_rank() OVER (PARTITION BY customer_id ORDER BY created_at) AS nth_order,
                pods_per_sku
         from prep
        where
            campaign_customer = 1
            and
            created_at >= min_campaign_order
        )
        ,

    -- ################################
    -- and transforming flavour rows into a one string
    -- ################################
     flavour_list_per_order as
        (
        select customer_id,
                order_number,
                created_at,
                nth_order,
               	sum(case
                    when nth_order = 1 then pods_per_sku else null
                end) as pods_in_first_order,
                --array_to_string(array_agg(DISTINCT flavour), ', ') AS flavour_list
                LISTAGG(DISTINCT flavour, ', ') AS flavour_list
        from campaign_orders
        -- why <=
        where nth_order <= 2
        group by
            customer_id,
            order_number,
            created_at,
            nth_order
        ),


    -- ################################
    -- obtaining second order's flavours
    -- ################################
    flavour_list_current_next_order as
        (select
            customer_id,
            order_number,
            created_at,
            flavour_list,
            pods_in_first_order,
            lead(flavour_list) over (partition by customer_id order by created_at asc, order_number asc) as flavour_list_next_order
        from
            flavour_list_per_order),
--
--    -- ################################
--    -- merging the order_line (flavours) with next & this flavour string
--    -- ################################
       final as (
            select
                a.customer_id,
                a.order_number,
                date(a.created_at) as "date",
                a.flavour,
                a.country,
                a.nth_order,
                b.flavour_list,
                b.flavour_list like '%' || a.flavour || '%' as flavour_in_first_order,
                b.flavour_list_next_order,
                b.flavour_list_next_order like '%' || a.flavour || '%' as flavour_in_next_order,
            	'offline' as source,
                b.pods_in_first_order
            from campaign_orders a
                left join flavour_list_current_next_order b
                on a.order_number = b.order_number
       )

select
	*
from
	final