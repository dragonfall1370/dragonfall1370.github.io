

with
    -- ################################
    -- gathering all orders enriched with campaign information and calculating direct revenue
    -- ################################
     order_enriched_direct_revenue as (
        select
           order_enriched.customer_id,
           order_enriched.created_at,
           order_enriched.id as order_id,
           order_enriched.net_revenue_2 as revenue,
           order_enriched.campaign_purchase,
           order_enriched.billing_address_country_code,
           -- calculating direct revenue
           case
                when order_enriched.campaign_purchase is true
                then order_enriched.net_revenue_2
           end as direct_campaign_revenue,
           -- identifying the timestamp of the camapign order
           max(case
               when order_enriched.campaign_purchase is true
               then order_enriched.created_at end) OVER (PARTITION BY order_enriched.customer_id)
           as min_campaign_purchase_date
        from "airup_eu_dwh"."reports"."fct_order_enriched_campaign_enriched_offline" order_enriched
        --from dbt_feldm.fct_order_enriched_campaign_enriched_offline order_enriched
        ),

    -- ################################
    -- calculating indirect revenue by using the min_campaign_purchase_date
    -- ################################
     order_enriched_overall as (
        select
            *,
            case
                when order_enriched_direct_revenue.min_campaign_purchase_date < order_enriched_direct_revenue.created_at
                and order_enriched_direct_revenue.min_campaign_purchase_date is not null
                then order_enriched_direct_revenue.revenue
            end as indirect_campaign_revenue
        from
            order_enriched_direct_revenue
     ),

     final as (
        select
            date_trunc('day', order_enriched_overall.created_at) as date,
            billing_address_country_code as country,
            sum(order_enriched_overall.direct_campaign_revenue) as direct_campaign_revenue,
            sum(order_enriched_overall.indirect_campaign_revenue) as indirect_campaign_revenue
        from
            order_enriched_overall
        group by 1,2
     )

select
     *
from
    final