----legacy:  amazon.mws_cohort_processing_separate_initial_purchase_global
---Authors: Etoma Egot

---###################################################################################################################

        ---compute mws_cohort_processing_separate_initial_purchase_global---

---###################################################################################################################

 

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

data_prep as
    (select distinct
        md5(buyer_email) as customer_id,
        amazon_order_id as order_number,
        sum(1) over (partition by md5(buyer_email) order by purchase_date asc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as nth_order,
        purchase_date as created_at,
        case
            when (sum(1) over (partition by md5(buyer_email) order by purchase_date asc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) = 1 and starter_sets >= 1
            then 1
            else 0
        end as init_order_starter_set,
        case
            when (sum(1) over (partition by md5(buyer_email) order by purchase_date asc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) = 1 and pods >= 1
            then 1
            else 0
        end as init_order_pods,
        lead(purchase_date) over (partition by md5(buyer_email) order by purchase_date asc) created_at_next_order,
        lead(purchase_date) over (partition by md5(buyer_email) order by purchase_date asc) - purchase_date as time_diff_next_order,
        min(date(date_trunc('month', purchase_date))) over (partition by md5(buyer_email)) as cohort,
        date_trunc('month', purchase_date) as month_created_at,
        
        -- ##############################
        -- calculate date diff in months
        (DATE_PART('year', purchase_date::date)
        - DATE_PART('year', (min(date_trunc('month', purchase_date)) over (partition by md5(buyer_email)))::date))
        * 12 +
        (DATE_PART('month', purchase_date::date)
        - DATE_PART('month', (min(date_trunc('month', purchase_date)) over (partition by md5(buyer_email)))::date)) as date_diff_months,
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
    WHERE ofse.returned IS FALSE -- filtering out returned orders
    group by
        md5(buyer_email),
        amazon_order_id,
        purchase_date,
        starter_sets,
        pods,
        loops
    order by
        md5(buyer_email) asc,
        purchase_date asc),

data_prep_2 as
    (select
        customer_id,
        order_number,
        nth_order,
        created_at,
        init_order_starter_set,
        init_order_pods,
        created_at_next_order,
        time_diff_next_order,
        cohort,
        month_created_at,
        case when nth_order = 1 then -1 else date_diff_months end as date_diff_months,
        net_revenue,
        starter_sets,
        pods,
        loops,
        sum(1) over (partition by customer_id order by created_at, order_number asc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as order_per_client,
        case
            when max(init_order_starter_set) over (partition by customer_id) = 1
            then 'yes'
            else 'no'
        end as init_order_incl_starter_set,
        case
            when max(init_order_pods) over (partition by customer_id) = 1
            then 'yes'
            else 'no'
        end as init_order_incl_pods,
        sum(pods) over (partition by customer_id) as pod_items_per_customer,
        -- count(pods) over (partition by customer_id) as pod_orders_per_customer
        count(pods) over (partition by customer_id, date_diff_months) as pod_orders_per_customer
    from
        data_prep),

data_prep_3 as
    (select
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
        data_prep_2
    group by
        customer_id,
        cohort,
        date_diff_months),

returning_customers as
    (select
        cohort,
        init_order_incl_starter_set,
        init_order_incl_pods,
        count(distinct case when order_per_client > 1 then customer_id else null end) as returning_customers,
        count(distinct case when order_per_client > 1 or (order_per_client = 1 and pods >= 1) then customer_id else null end) as returning_customers_pods_first
    from
        data_prep_2
    group by
        cohort,init_order_incl_starter_set,
        init_order_incl_pods)

select
    data_prep_3.cohort,
    date_diff_months,
    data_prep_3.init_order_incl_starter_set,
    data_prep_3.init_order_incl_pods,
    sum(net_revenue) as net_revenue,
    sum(pod_items_per_customer) / count(distinct customer_id) as avg_pod_items_per_customer,
    sum(pod_orders_per_customer) / count(distinct customer_id) as avg_pod_orders_per_customer,
    count(distinct customer_id) as nr_of_customers,
    extract(epoch from avg(time_diff_next_order)) / 86400 as avg_order_date_diff_days,
    sum(subsequent_orders)::bigint as subsequent_orders,
    sum(pod_orders_per_customer) as pod_orders_per_customer,
    returning_customers.returning_customers as returning_customers_cohort,
    returning_customers.returning_customers_pods_first as returning_customers_pods_first_cohort,
    sum(subsequent_orders_with_pods)::bigint as subsequent_orders_with_pods,
    sum(subsequent_orders_with_loops)::bigint as subsequent_orders_with_loops,

    sum(subsequent_orders_pods_first)::bigint as subsequent_orders_pods_first,
    sum(subsequent_orders_with_pods_or_loops)::bigint as subsequent_orders_with_pods_or_loops,
    sum(subsequent_orders_with_pods_pods_first)::bigint as subsequent_orders_with_pods_pods_first,
    sum(subsequent_orders_with_loops_pods_first)::bigint as subsequent_orders_with_loops_pods_first,
    sum(subsequent_orders_with_pods_or_loops_pods_first)::bigint as subsequent_orders_with_pods_or_loops_pods_first
from
    data_prep_3
left join returning_customers
on
    data_prep_3.cohort = returning_customers.cohort
    and
    data_prep_3.init_order_incl_starter_set = returning_customers.init_order_incl_starter_set
    and
    data_prep_3.init_order_incl_pods = returning_customers.init_order_incl_pods
group by
    data_prep_3.cohort,
    date_diff_months,
    data_prep_3.init_order_incl_starter_set,
    data_prep_3.init_order_incl_pods,
    returning_customers.returning_customers,
    returning_customers.returning_customers_pods_first
    --,pod_order
order by
    data_prep_3.cohort,
    date_diff_months