----legacy: amazon.mws_daily_net_revenue
---Author: Etoma Egot

---###################################################################################################################

                      ---Compute amazon.mws_daily_net_revenue--

---###################################################################################################################

 

with
prep_data as
    (select distinct
        amazon_order_id  as order_number,
        date(purchase_date) as "date",
        sum(coalesce(nullif(item_price, 'NaN'), 0) * coalesce(quantity_shipped, 0)) - sum(coalesce(nullif(item_promotion_discount, 'NaN'), 0) * coalesce(quantity_shipped, 0)) as net_revenue,
        sum(coalesce(nullif(shipping_price, 'NaN'), 0)) as shipping_revenue
    from
        "airup_eu_dwh"."amazon"."fct_orders_fulfilled_shipments_enriched" ofse
    where
        date(purchase_date) >= date_trunc('month', current_date - interval '1 month')
    group by
        amazon_order_id,
        date(purchase_date)),
        
refresh_date as
    (select
        date(max("_fivetran_synced")) as refresh_date
    from
        "airup_eu_dwh"."amazon"."fct_orders_fulfilled_shipments_enriched" ofse)

select
    "date",
    sum(net_revenue) as net_revenue,
    sum(shipping_revenue) as shipping_revenue,
    case
        when date_trunc('month', "date") = date_trunc('month', current_date)
        then 'current month'
        else 'previous month(s)'
    end as month_classification,
    case
        when date_part('day', "date") <= date_part('day', current_date)-1
        then 'MTD'
    end as mtd_qualifier,
    date_part('day', "date") as "day",
    count(distinct order_number) as orders,
    refresh_date.refresh_date
from
    prep_data
left join refresh_date on 1=1
group by
    "date",
    refresh_date.refresh_date
order by
    "date" desc