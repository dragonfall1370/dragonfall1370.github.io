

with 
    campaign_v1 as (
        with
    -- ################################
    -- gathering all orders enriched with campaign information and calculating direct revenue
    -- ################################
     order_enriched_direct_revenue as (
			SELECT
				dim_orders.campaign_name,
				order_enriched.customer_id,
			    order_enriched.created_at,
			    order_enriched.id as order_id,
			    order_enriched.net_revenue_2 as revenue,
			    case when order_enriched.id is not NULL then TRUE else FALSE end as campaign_purchase,
			    order_enriched.billing_address_country_code,
			    -- calculating direct revenue
			    order_enriched.net_revenue_2 as direct_campaign_revenue,
			    -- identifying the timestamp of the camapign order
				min(order_enriched.created_at) OVER (PARTITION BY order_enriched.customer_id) as min_campaign_purchase_date
			FROM
                "airup_eu_dwh"."shopify_global"."fct_order_enriched" order_enriched
				left join "airup_eu_dwh"."dbt_feldm"."dim_offline_campaign_orders" dim_orders on order_enriched.id = dim_orders.order_id
			where
				dim_orders.campaign_name = 'l-de_n-off_k-free-strap-v1'
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
    ),

    campaign_v2 as(
        with
    -- ################################
    -- gathering all orders enriched with campaign information and calculating direct revenue
    -- ################################
     order_enriched_direct_revenue as (
			SELECT
				dim_orders.campaign_name,
				order_enriched.customer_id,
			    order_enriched.created_at,
			    order_enriched.id as order_id,
			    order_enriched.net_revenue_2 as revenue,
			    case when order_enriched.id is not NULL then TRUE else FALSE end as campaign_purchase,
			    order_enriched.billing_address_country_code,
			    -- calculating direct revenue
			    order_enriched.net_revenue_2 as direct_campaign_revenue,
			    -- identifying the timestamp of the camapign order
				min(order_enriched.created_at) OVER (PARTITION BY order_enriched.customer_id) as min_campaign_purchase_date
			FROM
                "airup_eu_dwh"."shopify_global"."fct_order_enriched" order_enriched
				left join "airup_eu_dwh"."dbt_feldm"."dim_offline_campaign_orders" dim_orders on order_enriched.id = dim_orders.order_id
			where
				dim_orders.campaign_name = 'l-de_n-off_k-free-strap-v2'
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
    ),

    final as(
        select *, 'l-de_n-off_k-free-strap-v1' as campaign_name from campaign_v1 
        union all
        select *, 'l-de_n-off_k-free-strap-v2' as campaign_name from campaign_v2
    )

select * from final