

with 
    campaign_v1 as (
        with
    -- ################################
    -- gathering all orders enriched with campaign information and calculating direct revenue
    -- ################################
     	all_orders as
		(select
			dim_orders.campaign_name,
			order_enriched.customer_id,
			order_enriched.created_at,
		    order_enriched.billing_address_country_code as country,
			order_enriched.order_number,
		    case when order_enriched.id is not NULL then TRUE else FALSE end as campaign_purchase,
	        min(order_enriched.created_at) OVER (PARTITION BY order_enriched.customer_id ORDER BY order_enriched.created_at rows between 1 following and 1 following) AS min_order_date_per_customer,
		    max(CASE WHEN campaign_purchase is TRUE then 1 ELSE 0
                END) OVER (PARTITION BY order_enriched.customer_id) as campaign_customer,
		    min(CASE WHEN campaign_purchase is TRUE then created_at
                END) OVER (PARTITION BY order_enriched.customer_id) as min_campaign_order,
            order_enriched.net_revenue_2 as revenue
			FROM
				"airup_eu_dwh"."shopify_global"."fct_order_enriched" order_enriched
				left join "airup_eu_dwh"."dbt_feldm"."dim_offline_campaign_orders" dim_orders on order_enriched.id = dim_orders.order_id
			where
				--order_enriched.id = dim_orders.order_id and
				order_enriched.financial_status in ('paid', 'partially_refunded') and
				dim_orders.campaign_name = 'l-de_n-off_k-free-strap-v1'
				),

    -- ################################
    -- flagging customers whether they returned or not
    -- ################################
    orders_campaign_customers as (
        select
            customer_id,
            created_at,
            country,
            order_number,
            revenue,
            min_campaign_order,
            case
                when count(order_number) over (PARTITION BY customer_id) = 1 then 'single customer'
                when count(order_number) over (PARTITION BY customer_id) >= 2 then 'returning customer'
            end as returning_customer
        from
           all_orders
        where
            campaign_customer = 1
            and created_at >= min_campaign_order
    ),

    -- ################################
    -- allocating customers based on their min_campaign_order (first chart)
    -- ################################
    returning_campaign_customers as (
        select
            country,
            returning_customer,
            date_trunc('day', created_at) as date,
            count(distinct customer_id) as customers,
            sum(revenue) as revenue_first_campaign_order,
            count(DISTINCT order_number) AS first_campaign_orders
        from
            orders_campaign_customers
        where
            created_at = min_campaign_order
        group by
            1,2,3
    ),

    -- ################################
    -- preparing the remaining metrics (orders, revenue)
    -- ################################
    returning_campaign_orders_and_revenue as (
        select
            country,
            returning_customer,
            date_trunc('day', created_at) as date,
            sum(revenue) as revenue,
            count(distinct order_number) as orders
        from
            orders_campaign_customers
        group by
            1,2,3
    ),
    -- ################################
    -- merging the previous CTEs two a final dataset
    -- ################################
    final as (
        select
            a.*,
            b.customers,
            b.revenue_first_campaign_order,
            b.first_campaign_orders
        from
           returning_campaign_orders_and_revenue a
           left join returning_campaign_customers b
               on a.country = b.country
               and a.returning_customer = b.returning_customer
               and a.date = b.date
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
     	all_orders as
		(select
			dim_orders.campaign_name,
			order_enriched.customer_id,
			order_enriched.created_at,
		    order_enriched.billing_address_country_code as country,
			order_enriched.order_number,
		    case when order_enriched.id is not NULL then TRUE else FALSE end as campaign_purchase,
	        min(order_enriched.created_at) OVER (PARTITION BY order_enriched.customer_id ORDER BY order_enriched.created_at rows between 1 following and 1 following) AS min_order_date_per_customer,
		    max(CASE WHEN campaign_purchase is TRUE then 1 ELSE 0
                END) OVER (PARTITION BY order_enriched.customer_id) as campaign_customer,
		    min(CASE WHEN campaign_purchase is TRUE then created_at
                END) OVER (PARTITION BY order_enriched.customer_id) as min_campaign_order,
            order_enriched.net_revenue_2 as revenue
			FROM
				"airup_eu_dwh"."shopify_global"."fct_order_enriched" order_enriched
				left join "airup_eu_dwh"."dbt_feldm"."dim_offline_campaign_orders" dim_orders on order_enriched.id = dim_orders.order_id
			where
				--order_enriched.id = dim_orders.order_id and
				order_enriched.financial_status in ('paid', 'partially_refunded') and
				dim_orders.campaign_name = 'l-de_n-off_k-free-strap-v2'
				),

    -- ################################
    -- flagging customers whether they returned or not
    -- ################################
    orders_campaign_customers as (
        select
            customer_id,
            created_at,
            country,
            order_number,
            revenue,
            min_campaign_order,
            case
                when count(order_number) over (PARTITION BY customer_id) = 1 then 'single customer'
                when count(order_number) over (PARTITION BY customer_id) >= 2 then 'returning customer'
            end as returning_customer
        from
           all_orders
        where
            campaign_customer = 1
            and created_at >= min_campaign_order
    ),

    -- ################################
    -- allocating customers based on their min_campaign_order (first chart)
    -- ################################
    returning_campaign_customers as (
        select
            country,
            returning_customer,
            date_trunc('day', created_at) as date,
            count(distinct customer_id) as customers,
            sum(revenue) as revenue_first_campaign_order,
            count(DISTINCT order_number) AS first_campaign_orders
        from
            orders_campaign_customers
        where
            created_at = min_campaign_order
        group by
            1,2,3
    ),

    -- ################################
    -- preparing the remaining metrics (orders, revenue)
    -- ################################
    returning_campaign_orders_and_revenue as (
        select
            country,
            returning_customer,
            date_trunc('day', created_at) as date,
            sum(revenue) as revenue,
            count(distinct order_number) as orders
        from
            orders_campaign_customers
        group by
            1,2,3
    ),
    -- ################################
    -- merging the previous CTEs two a final dataset
    -- ################################
    final as (
        select
            a.*,
            b.customers,
            b.revenue_first_campaign_order,
            b.first_campaign_orders
        from
           returning_campaign_orders_and_revenue a
           left join returning_campaign_customers b
               on a.country = b.country
               and a.returning_customer = b.returning_customer
               and a.date = b.date
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