

with 
    campaign_v1 as (
        WITH
    -- ################################
    -- gathering all oders enriched by campaign information
    -- flagging customers who purchased via campaign
    -- ################################
	all_orders as
		(select
			order_enriched.customer_id,
			order_enriched.created_at,
		    order_enriched.billing_address_country_code as country,
			order_enriched.order_number,
		    case when campaign_orders.order_id is not NULL then TRUE else FALSE end as campaign_purchase,
	        min(order_enriched.created_at) OVER (PARTITION BY order_enriched.customer_id ORDER BY order_enriched.created_at rows between 1 following and 1 following ) AS min_order_date_per_customer,
		    max(CASE WHEN campaign_purchase is TRUE then 1 ELSE 0
                END) OVER (PARTITION BY order_enriched.customer_id) as campaign_customer,
            order_enriched.net_revenue_2 as revenue
		from
		    -- todo update the source (somehow not working)
		    --"airup_eu_dwh"."reports"."fct_order_enriched_campaign_enriched_offline" order_enriched
			--dbt_feldm.fct_order_enriched_campaign_enriched_offline
			"airup_eu_dwh"."shopify_global"."fct_order_enriched" order_enriched
        	LEFT JOIN "airup_eu_dwh"."dbt_feldm"."dim_offline_campaign_orders" campaign_orders
        		on order_enriched.id = campaign_orders.order_id
        		and campaign_orders.campaign_name = 'l-de_n-off_k-free-strap-v1'
		),

    -- ################################
    -- flagging customers whether they returned or not
    -- ################################
    orders_non_campaign_customers as (
        select
            customer_id,
            created_at,
            country,
            order_number,
            revenue,
            min_order_date_per_customer,
            case
                when count(order_number) over (PARTITION BY customer_id) = 1 then 'single customer'
                when count(order_number) over (PARTITION BY customer_id) >= 2 then 'returning customer'
            end as returning_customer
        from
           all_orders
        where
            campaign_customer = 0
    ),

    -- ################################
    -- allocating customers based on their min_campaign_order (first chart)
    -- ################################
    returning_non_campaign_customers as (
        select
            country,
            returning_customer,
            date_trunc('day', created_at) as date,
            count(distinct customer_id) as customers
        from
            orders_non_campaign_customers
        where
            created_at = min_order_date_per_customer
        group by
            1,2,3
    ),

    -- ################################
    -- preparing the remaining metrics (orders, revenue)
    -- ################################
    returning_non_campaign_orders_and_revenue as (
        select
            country,
            returning_customer,
            date_trunc('day', created_at) as date,
            sum(revenue) as revenue,
            count(distinct order_number) as orders
        from
            orders_non_campaign_customers
        group by
            1,2,3
    ),

    -- ################################
    -- merging the previous CTEs two a final dataset
    -- using the campaign_customer query as a spine to remove redundant data
    -- ################################
    final as (
        select
            a.date,
            a.country,
            a.returning_customer,
            b.revenue,
            b.orders,
            c.customers
        from
            --dbt_feldm.dim_offline_campaign_returning_customers a
            "airup_eu_dwh"."reports"."fct_campaign_returning_customers_offline" a
            left join returning_non_campaign_orders_and_revenue b
                on a.date = b.date
                and a.country = b.country
                and a.returning_customer = b.returning_customer
            left join returning_non_campaign_customers c
                on a.date = c.date
                and a.country = c.country
                and a.returning_customer = c.returning_customer
    )
select * from final
    ),

    campaign_v2 as(
        WITH
    -- ################################
    -- gathering all oders enriched by campaign information
    -- flagging customers who purchased via campaign
    -- ################################
	all_orders as
		(select
			order_enriched.customer_id,
			order_enriched.created_at,
		    order_enriched.billing_address_country_code as country,
			order_enriched.order_number,
		    case when campaign_orders.order_id is not NULL then TRUE else FALSE end as campaign_purchase,
	        min(order_enriched.created_at) OVER (PARTITION BY order_enriched.customer_id ORDER BY order_enriched.created_at rows between 1 following and 1 following ) AS min_order_date_per_customer,
		    max(CASE WHEN campaign_purchase is TRUE then 1 ELSE 0
                END) OVER (PARTITION BY order_enriched.customer_id) as campaign_customer,
            order_enriched.net_revenue_2 as revenue
		from
		    -- todo update the source (somehow not working)
		    --"airup_eu_dwh"."reports"."fct_order_enriched_campaign_enriched_offline" order_enriched
			--dbt_feldm.fct_order_enriched_campaign_enriched_offline
			"airup_eu_dwh"."shopify_global"."fct_order_enriched" order_enriched
        	LEFT JOIN "airup_eu_dwh"."dbt_feldm"."dim_offline_campaign_orders" campaign_orders
        		on order_enriched.id = campaign_orders.order_id
        		and campaign_orders.campaign_name = 'l-de_n-off_k-free-strap-v2'
		),

    -- ################################
    -- flagging customers whether they returned or not
    -- ################################
    orders_non_campaign_customers as (
        select
            customer_id,
            created_at,
            country,
            order_number,
            revenue,
            min_order_date_per_customer,
            case
                when count(order_number) over (PARTITION BY customer_id) = 1 then 'single customer'
                when count(order_number) over (PARTITION BY customer_id) >= 2 then 'returning customer'
            end as returning_customer
        from
           all_orders
        where
            campaign_customer = 0
    ),

    -- ################################
    -- allocating customers based on their min_campaign_order (first chart)
    -- ################################
    returning_non_campaign_customers as (
        select
            country,
            returning_customer,
            date_trunc('day', created_at) as date,
            count(distinct customer_id) as customers
        from
            orders_non_campaign_customers
        where
            created_at = min_order_date_per_customer
        group by
            1,2,3
    ),

    -- ################################
    -- preparing the remaining metrics (orders, revenue)
    -- ################################
    returning_non_campaign_orders_and_revenue as (
        select
            country,
            returning_customer,
            date_trunc('day', created_at) as date,
            sum(revenue) as revenue,
            count(distinct order_number) as orders
        from
            orders_non_campaign_customers
        group by
            1,2,3
    ),

    -- ################################
    -- merging the previous CTEs two a final dataset
    -- using the campaign_customer query as a spine to remove redundant data
    -- ################################
    final as (
        select
            a.date,
            a.country,
            a.returning_customer,
            b.revenue,
            b.orders,
            c.customers
        from
            --dbt_feldm.dim_offline_campaign_returning_customers a
            "airup_eu_dwh"."reports"."fct_campaign_returning_customers_offline" a
            left join returning_non_campaign_orders_and_revenue b
                on a.date = b.date
                and a.country = b.country
                and a.returning_customer = b.returning_customer
            left join returning_non_campaign_customers c
                on a.date = c.date
                and a.country = c.country
                and a.returning_customer = c.returning_customer
    )
select * from final   
    ),

    final as(
        select *, 'l-de_n-off_k-free-strap-v1' as campaign_name from campaign_v1 
        union all
        select *, 'l-de_n-off_k-free-strap-v2' as campaign_name from campaign_v2
    )

select * from final