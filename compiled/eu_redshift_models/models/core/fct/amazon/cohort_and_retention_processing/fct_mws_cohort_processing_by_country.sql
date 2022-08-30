----legacy:  amazon.mws_cohort_processing_by_country
---Authors: Etoma Egot

---###################################################################################################################

        ---compute mws_cohort_processing_by_country---

---###################################################################################################################

 

-- Same view as shopify_cohort_processing.sql in "sql_scripts" (Stage 1), but with additional columns which count pods in init order as "subsequent purchase".
-- 2021-04-23 replaced table name "orders_fullfilled_shipments_manual_upload" with "orders_fulfilled_shipments_manual_upload_enriched", added country dimension
-- 2021-12-26 replaced table name "orders_fullfilled_shipments_manual_upload_enriched" with "orders_fulfilled_shipments_enriched"

with
product_categories_per_order as
    (select
        amazon_order_id as "oid",
        sum(case when product_type = 'Starter Set' then quantity_shipped end) as starter_sets,
        sum(case when product_type = 'Pods' then quantity_shipped end) as pods,
        sum(case when product_type = 'Accessories' then quantity_shipped end) as loops
    from
        "airup_eu_dwh"."amazon"."fct_orders_fulfilled_shipments_enriched" ofse
    group by
        amazon_order_id),

data_prep_by_country as
    (select distinct
        country_abbreviation,
        country_fullname,
        md5(buyer_email) as customer_id,
        amazon_order_id as order_number,
        purchase_date as created_at,
        case
            when (sum(1) over (partition by md5(buyer_email), country_abbreviation order by purchase_date asc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) = 1 and starter_sets >= 1
            then 1
            else 0
        end as init_order_starter_set,
        case
            when (sum(1) over (partition by md5(buyer_email), country_abbreviation order by purchase_date asc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) = 1 and pods >= 1
            then 1
            else 0
        end as init_order_pods,
        lead(purchase_date) over (partition by md5(buyer_email), country_abbreviation order by purchase_date asc) created_at_next_order,
        lead(purchase_date) over (partition by md5(buyer_email), country_abbreviation order by purchase_date asc) - purchase_date as time_diff_next_order,
        min(date(date_trunc('month', purchase_date))) over (partition by md5(buyer_email), country_abbreviation) as cohort,
        date_trunc('month', purchase_date) as month_created_at,
        
        -- ##############################
        -- calculate date diff in months
        (DATE_PART('year', purchase_date::date)
        - DATE_PART('year', (min(date_trunc('month', purchase_date)) over (partition by md5(buyer_email), country_abbreviation))::date))
        * 12 +
        (DATE_PART('month', purchase_date::date)
        - DATE_PART('month', (min(date_trunc('month', purchase_date)) over (partition by md5(buyer_email), country_abbreviation))::date)) as date_diff_months,
        -- ##############################
        
        sum(coalesce(nullif(item_price, 'NaN'), 0) * coalesce(quantity_shipped, 0)) - sum(coalesce(nullif(item_promotion_discount, 'NaN'), 0) * coalesce(quantity_shipped, 0)) as net_revenue,
        starter_sets,
        pods,
        loops,
        pods >= 1 as "pod_order"
    from
        "airup_eu_dwh"."amazon"."fct_orders_fulfilled_shipments_enriched" ofse
    left join product_categories_per_order
        on ofse.amazon_order_id = product_categories_per_order."oid"
    WHERE  ofse.returned IS FALSE -- filtering out returned orders
    group by
        md5(buyer_email),
        country_abbreviation,
        country_fullname,
        amazon_order_id,
        purchase_date,
        starter_sets,
        pods,
        loops
--  order by
--      md5(buyer_email) asc,
--      purchase_date asc
        ),

data_prep_2_by_country as
    (select
        data_prep_by_country.*,
        sum(1) over (partition by customer_id, country_abbreviation order by created_at, order_number asc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as order_per_client,
        case
            when max(init_order_starter_set) over (partition by customer_id, country_abbreviation) = 1
            then 'yes'
            else 'no'
        end as init_order_incl_starter_set,
        case
            when max(init_order_pods) over (partition by customer_id, country_abbreviation) = 1
            then 'yes'
            else 'no'
        end as init_order_incl_pods,
        sum(pods) over (partition by customer_id, country_abbreviation) as pod_items_per_customer,
        -- count(pods) over (partition by customer_id) as pod_orders_per_customer
        count(pods) over (partition by customer_id, country_abbreviation, date_diff_months) as pod_orders_per_customer
    from
        data_prep_by_country),

data_prep_3_by_country as
    (select
        country_abbreviation,
        country_fullname,
        customer_id,
        cohort,
        date_diff_months,
        max(init_order_incl_starter_set) as init_order_incl_starter_set,
        max(init_order_incl_pods) as init_order_incl_pods,
        sum(net_revenue) as net_revenue,
        max(pod_items_per_customer) as pod_items_per_customer,
        max(pod_orders_per_customer) as pod_orders_per_customer,
        avg(time_diff_next_order) as time_diff_next_order,
        count(distinct case when order_per_client > 1 then order_number else null end) as subsequent_orders
        , count(distinct case when order_per_client > 1 and pods >= 1 then order_number else null end) as subsequent_orders_with_pods
        , count(distinct case when order_per_client > 1 and loops >= 1 then order_number else null end) as subsequent_orders_with_loops
        , count(distinct case when order_per_client > 1 and (pods >= 1 or loops >= 1) then order_number else null end) as subsequent_orders_with_pods_or_loops
        
        , count(distinct case when order_per_client > 1 or (order_per_client = 1 and pods >= 1) then order_number else null end) as subsequent_orders_pods_first
        , count(distinct case when order_per_client > 0 and pods >= 1 then order_number else null end) as subsequent_orders_with_pods_pods_first
        , count(distinct case when order_per_client > 0 and loops >= 1 then order_number else null end) as subsequent_orders_with_loops_pods_first
        , count(distinct case when order_per_client > 0 and (pods >= 1 or loops >= 1) then order_number else null end) as subsequent_orders_with_pods_or_loops_pods_first
    
    from
        data_prep_2_by_country
    group by
        country_abbreviation,
        country_fullname,
        customer_id,
        cohort,
        date_diff_months),

returning_customers_by_country as
    (select
        country_abbreviation,
        country_fullname,
        cohort,
        init_order_incl_starter_set,
        init_order_incl_pods,
        count(distinct case when order_per_client > 1 then customer_id else null end) as returning_customers,
        count(distinct case when order_per_client > 1 or (order_per_client = 1 and pods >= 1) then customer_id else null end) as returning_customers_pods_first
    from
        data_prep_2_by_country
    group by
        country_abbreviation,
        country_fullname,
        cohort,init_order_incl_starter_set,
        init_order_incl_pods)

select
    data_prep_3_by_country.country_abbreviation,
    data_prep_3_by_country.country_fullname,
    data_prep_3_by_country.cohort,
    date_diff_months,
    data_prep_3_by_country.init_order_incl_starter_set,
    data_prep_3_by_country.init_order_incl_pods,
    sum(net_revenue) as net_revenue,
    sum(pod_items_per_customer) / count(distinct customer_id) as avg_pod_items_per_customer,
    sum(pod_orders_per_customer) / count(distinct customer_id) as avg_pod_orders_per_customer,
    count(distinct customer_id) as nr_of_customers,
    extract(epoch from avg(time_diff_next_order)) / 86400 as avg_order_date_diff_days,
    sum(subsequent_orders)::bigint as subsequent_orders,
    sum(pod_orders_per_customer) as pod_orders_per_customer,
    returning_customers_by_country.returning_customers as returning_customers_cohort,
    returning_customers_by_country.returning_customers_pods_first as returning_customers_pods_first_cohort,
    sum(subsequent_orders_with_pods)::bigint as subsequent_orders_with_pods,
    sum(subsequent_orders_with_loops)::bigint as subsequent_orders_with_loops,
    
    sum(subsequent_orders_pods_first)::bigint as subsequent_orders_pods_first,
    sum(subsequent_orders_with_pods_or_loops)::bigint as subsequent_orders_with_pods_or_loops,
    sum(subsequent_orders_with_pods_pods_first)::bigint as subsequent_orders_with_pods_pods_first,
    sum(subsequent_orders_with_loops_pods_first)::bigint as subsequent_orders_with_loops_pods_first,
    sum(subsequent_orders_with_pods_or_loops_pods_first)::bigint as subsequent_orders_with_pods_or_loops_pods_first
from
    data_prep_3_by_country
left join returning_customers_by_country on
    data_prep_3_by_country.country_abbreviation = returning_customers_by_country.country_abbreviation
    and
    data_prep_3_by_country.country_fullname = returning_customers_by_country.country_fullname
    and
    data_prep_3_by_country.cohort = returning_customers_by_country.cohort
    and
    data_prep_3_by_country.init_order_incl_starter_set = returning_customers_by_country.init_order_incl_starter_set
    and
    data_prep_3_by_country.init_order_incl_pods = returning_customers_by_country.init_order_incl_pods
group by
    data_prep_3_by_country.country_abbreviation,
    data_prep_3_by_country.country_fullname,
    data_prep_3_by_country.cohort,
    date_diff_months,
    data_prep_3_by_country.init_order_incl_starter_set,
    data_prep_3_by_country.init_order_incl_pods,
    returning_customers_by_country.returning_customers,
    returning_customers_by_country.returning_customers_pods_first