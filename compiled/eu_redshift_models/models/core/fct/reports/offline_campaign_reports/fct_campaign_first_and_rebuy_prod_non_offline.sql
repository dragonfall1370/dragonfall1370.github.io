

with
    -- ################################
    -- gathering all orders enriched by campaign information
    -- flagging customers who purchased via campaign
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
                dense_rank() OVER (PARTITION BY customer_id ORDER BY created_at) AS nth_order
         from prep
        where
            campaign_customer = 1
            and
            created_at >= min_campaign_order
        ),

    -- ################################
    -- and transforming flavour rows into a one string
    -- ################################
     flavour_list_per_order as
        (select customer_id,
                order_number,
                created_at,
                nth_order,
                --array_to_string(array_agg(DISTINCT flavour), ', ') AS flavour_list
                LISTAGG(DISTINCT flavour, ', ') AS flavour_list
        from campaign_orders
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
            lead(flavour_list) over (partition by customer_id order by created_at asc, order_number asc) as flavour_list_next_order
        from
            flavour_list_per_order),

    -- ################################
    -- merging the order_line (flavours) with next & this flavour string
    -- ################################
    flavour_list_comparison as (
        select
            a.customer_id,
            a.order_number,
            a.created_at,
            a.flavour,
            a.country,
            a.nth_order,
            b.flavour_list,
            b.flavour_list like '%' || a.flavour || '%' as flavour_in_first_order,
            b.flavour_list_next_order,
          b.flavour_list_next_order like '%' || a.flavour || '%' as flavour_in_next_order
        from campaign_orders a
            left join flavour_list_current_next_order b
            on a.order_number = b.order_number
    ),

    -- ################################
    -- calculating the share of flavours in first order
    -- ################################

    number_of_first_orders as (
        select count(distinct order_number) as order_count from flavour_list_comparison where nth_order = 1
        ),

    first_order_content as (
        select flavour,
               count(distinct case when flavour_in_first_order then order_number end)
                   / b.order_count::float as first_order_flavour_share
        from flavour_list_comparison
                 left join number_of_first_orders b
                    on 1 = 1
        where nth_order = 1
        group by 1,order_count
    ),

    -- ################################
    -- calculating the share of flavours in the subsequent order
    -- ################################
    subsequent_order_retention as (
        select
            flavour,
            count(distinct case when flavour_in_next_order then order_number end) / count(order_number)::float as subsequent_order_retention_rate
        from
            flavour_list_comparison
        where
            flavour_in_next_order is not null
            and nth_order = 1
        group by
            flavour
    ),

    -- ################################
    -- joining info from first order and subsequent order together
    -- ################################
    final as (
        select
            a.flavour,
            a.subsequent_order_retention_rate,
            b.first_order_flavour_share
        from subsequent_order_retention a
            left join first_order_content b
                on a.flavour = b.flavour
    )

select
     *
from
     final