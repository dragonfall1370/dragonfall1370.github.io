

with
    main_groupby AS (
        SELECT 
        pod_flavour_first::text,
        country_abbreviation::text,
        starter_set_flag::text AS product_type,
        'quarter'::text AS timeframe,
        count(DISTINCT customer_id)::bigint AS customers,
        count(DISTINCT
            CASE
                WHEN returning_customer_7_days = 1 THEN customer_id
                ELSE NULL
            END)::bigint AS returning_customers_7_days,
        count(DISTINCT
            CASE
                WHEN returning_customer_30_days = 1 THEN customer_id
                ELSE NULL
            END)::bigint AS returning_customers_30_days,
        count(DISTINCT
            CASE
                WHEN returning_customer_90_days = 1 THEN customer_id
                ELSE NULL
            END)::bigint AS returning_customers_90_days,
        count(DISTINCT
            CASE
                WHEN returning_customer_overall = 1 THEN customer_id
                ELSE NULL
            END)::bigint AS returning_customer_overall
        FROM "airup_eu_dwh"."flavour_analysis"."fct_returning_customers_by_first_flavour_processing" 
        WHERE customers_this_quarter = 1
        GROUP BY pod_flavour_first, country_abbreviation, starter_set_flag
    ),

   country_groupby as (
        select
            pod_flavour_first::text,
            country_abbreviation::text,
            'All'::text as product_type,
            timeframe::text,
            sum(customers),
            sum(returning_customers_7_days),
            sum(returning_customers_30_days),
            sum(returning_customers_90_days),
            sum(returning_customer_overall)
        from 
            main_groupby
        group by 
            1,2,3,4
    ),

   product_groupby as (
        select
            pod_flavour_first::text,
            'All'::text as country_abbreviation,
            product_type::text,
            timeframe::text,
            sum(customers),
            sum(returning_customers_7_days),
            sum(returning_customers_30_days),
            sum(returning_customers_90_days),
            sum(returning_customer_overall)
        from 
            main_groupby
        group by 
            1,2,3,4
    ),

   full_groupby as (
        select
            pod_flavour_first::text,
            'All'::text as country_abbreviation,
            'All'::text as product_type,
            timeframe::text,
            sum(customers),
            sum(returning_customers_7_days),
            sum(returning_customers_30_days),
            sum(returning_customers_90_days),
            sum(returning_customer_overall)
        from 
            main_groupby
        group by 
            1,2,3,4
    ),

    union_cte as (
    select * from main_groupby
    union all
    select * from country_groupby
    union all
    select * from product_groupby
    union all
    select * from full_groupby)

select
    *
from
    union_cte