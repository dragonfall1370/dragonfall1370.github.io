

with
    -- ################################
    -- gathering all orders enriched by campaign information
    -- flagging customers who purchased via campaign
    -- ################################
    all_orders as
        (select
            customer_id,
            id as order_id,
            created_at,
            billing_address_country_code as country,
            min(created_at) OVER (PARTITION BY customer_id ORDER BY created_at rows between 1 following and 1 following ) AS min_order_date_per_customer,
		    max(CASE WHEN campaign_purchase is TRUE then 1 ELSE 0
                END) OVER (PARTITION BY customer_id) as campaign_customer,
		    min(CASE WHEN campaign_purchase is TRUE then created_at
                END) OVER (PARTITION BY customer_id) as min_campaign_order
		from
		    -- todo update the source (somehow not working)
		    "airup_eu_dwh"."reports"."fct_order_enriched_campaign_enriched_offline" order_enriched
			--dbt_feldm.fct_order_enriched_campaign_enriched_offline
        where
            created_at > '2021-10-01' -- reducing the amount of data (campaign was launched after 10/2021)
        ),

    -- ################################
    -- flagging customers whether they returned or not
    -- ################################
    orders_campaign_customers as (
        select
            customer_id,
            created_at,
            country,
            order_id,
            min_campaign_order
        from
           all_orders
        where
            campaign_customer = 1
            and created_at >= min_campaign_order
    ),
        -- ################################
    -- prepping the dataset for metric calculation
    -- ################################
    data_prep_by_country as
        (select distinct
            country,
            customer_id,
            order_id,
            min_campaign_order,
            created_at,
            sum(1) over (partition by customer_id, country order by created_at asc rows between 1 following and 1 following ) as nth_order,
            lead(created_at) over (partition by customer_id, country order by created_at asc ) - created_at as time_diff_next_order,
            --min(date(date_trunc('week', created_at))) over (partition by customer_id, country) as cohort,
            min(date(date_trunc('month', created_at))) over (partition by customer_id, country) as cohort,
            --date_trunc('week', created_at) as week_created_at,
            date_trunc('month', created_at) as week_created_at,
            -- ##############################
            -- calculate date diff in weeks
            case
                when created_at = min_campaign_order then '0' -- for the very first order
                --when date_trunc('week', created_at) = date_trunc('week', min_campaign_order) then '0+' -- orders which happened in the same week as first order
                when date_trunc('month', created_at) = date_trunc('month', min_campaign_order) then '0+' -- orders which happened in the same month as first order
                --else cast(TRUNC(DATE_PART('day', created_at - min_campaign_order)/7) as varchar)				
                --else cast(TRUNC((cast(created_at as date) - cast(min_campaign_order as date))/7) as varchar)
                else cast(TRUNC((cast(created_at as date) - cast(min_campaign_order as date))/31) as varchar)
            --end as date_diff_weeks
            end as date_diff_months
            -- ##############################
        from
            orders_campaign_customers
        group by
            customer_id,
            country,
            order_id,
            created_at,
            min_campaign_order
            ),

    -- ################################
    -- calculating cohort size
    -- ################################
    cohort_size as
        (select
            cohort,
            country,
            count(distinct customer_id) as cohort_size
        from
            data_prep_by_country
        where
            --date_diff_weeks = '0'
            date_diff_months = '0'
        group by 1,2
            ),

    -- ################################
    -- final aggregation and metric calculation
    -- bringing in cohort size
    -- ################################
    final as
        (select
            a.cohort,
            a.country,
            --a.date_diff_weeks,
            a.date_diff_months,
            b.cohort_size,
            --date_trunc('week', a.created_at) as month,
            date_trunc('month', a.created_at) as month,
            count(distinct a.customer_id) as customers,
            count(distinct a.customer_id)::float / b.cohort_size::float  as retention_rate,
            count(distinct a.order_id)  as orders,
            count(distinct a.order_id)::float / b.cohort_size::float as rebuy_rate
         from
            data_prep_by_country a
                left join cohort_size b
                    on a.cohort = b.cohort
                    and a.country = b.country
         group by 1,2,3,4,5
        )

select
    *
from
    final