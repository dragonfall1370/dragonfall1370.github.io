----legacy: amazon.mws_daily_product_revenue
---Author: Etoma Egot

---###################################################################################################################

                      ---Compute amazon.mws_daily_product_revenue--

---###################################################################################################################

 

with
product_categories as
    (select distinct
        product_name,
        case
            when product_name ilike '%starter-set%' then 'starter-sets'
            when product_name ilike '%pod%' then 'pods'
            when product_name ilike '%silikon-schlaufe%' then 'loops'
        end as category
    from
        "airup_eu_dwh"."amazon"."fct_orders_fulfilled_shipments_enriched" ofse
    ),
        
prep_data as
    (select distinct
        amazon_order_id as order_number,
        date(purchase_date) as "date",
        ofse.product_name as product_name,
        product_categories.category,
        sum(coalesce(nullif(item_price, 'NaN'), 0) * coalesce(quantity_shipped, 0)) - sum(coalesce(nullif(item_promotion_discount, 'NaN'), 0) * coalesce(quantity_shipped, 0)) as product_revenue,
        sum(item_tax) as product_tax
    from
        "airup_eu_dwh"."amazon"."fct_orders_fulfilled_shipments_enriched" ofse
    left join product_categories
        on ofse.product_name = product_categories.product_name
    where
        date(purchase_date) >= date_trunc('month', current_date)
    group by
        amazon_order_id,
        date(purchase_date),
        ofse.product_name,
        product_categories.category)

select
    "date",
    category,
    -- sum(product_revenue) as product_revenue,
    -- sum(product_tax) as taxes,
    sum(product_revenue) as product_net_revenue,
    product_name
from
    prep_data
group by
    "date",
    category,
    product_name