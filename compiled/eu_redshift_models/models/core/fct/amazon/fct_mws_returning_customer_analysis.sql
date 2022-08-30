----legacy:  amazon.mws_returning_customer_analysis
---Authors: Etoma Egot

---###################################################################################################################

        ---compute amazon.mws_returning_customer_analysis---

---###################################################################################################################

 

WITH
initial_orders as
    (select * from (
select
        md5(buyer_email),
        purchase_date as init_order_timestamp,
        date_trunc('month', purchase_date) as init_order_cohort_month, 
        amazon_order_id as init_order_number,
        rank() OVER (PARTITION BY md5(buyer_email) ORDER BY purchase_date,
        date_trunc('month', purchase_date), 
        amazon_order_id) AS rank_id
 from "airup_eu_dwh"."amazon"."fct_orders_fulfilled_shipments_enriched")  AS ranked
        where ranked.rank_id = 1
    ),
distinct_orders as
    (select distinct
        amazon_order_id as order_number,
        purchase_date as created_at,
        --total_price,
        --total_tax,
        --shipping_lines_price_set_shop_money_amount as total_shipping_cost,
        sum(coalesce(nullif(item_price, 'NaN'), 0) * coalesce(quantity_shipped, 0)) - sum(coalesce(nullif(item_promotion_discount, 'NaN'), 0) * coalesce(quantity_shipped, 0)) as net_revenue
    from
        "airup_eu_dwh"."amazon"."fct_orders_fulfilled_shipments_enriched" ofse
    group by
        amazon_order_id,
        purchase_date),
        
refresh_date as
    (select
        date(max("_fivetran_synced")) as refresh_date
    from
        "airup_eu_dwh"."amazon"."fct_orders_fulfilled_shipments_enriched" ofse)

select
    case when init_order_number is null then 'Returning Amazon Customer' else 'New Amazon Customer' end as returning_customer_flag,
    date(date_trunc('month', created_at)) as month,
    date(created_at) as order_date,
    count(distinct order_number) as orders,
    sum(net_revenue) as net_revenue,
    refresh_date.refresh_date,
    case
        when date_trunc('month', created_at) = date_trunc('month', current_date) then 'current month'
        else 'previous month(s)'
    end as month_classification
from
    distinct_orders
left join initial_orders on distinct_orders.order_number = initial_orders.init_order_number
left join refresh_date on 1=1
group by
    case when init_order_number is null then 'Returning Amazon Customer' else 'New Amazon Customer' end,
    date_trunc('month', created_at),
    date(created_at),
    refresh_date.refresh_date