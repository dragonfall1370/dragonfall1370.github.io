

select
    a.country,
    a.returning_customer,
    a.date,
    a.revenue as campaign_revenue,
    a.orders as campaign_orders,
    a.customers as campaign_customers,
    b.revenue as non_campaign_revenue,
    b.orders as non_campaign_orders,
    b.customers as non_campaign_customers
from
    --dbt_feldm.fct_campaign_returning_customers_offline a
    "airup_eu_dwh"."reports"."fct_campaign_returning_customers_offline" a
    --left join  dbt_feldm.dim_non_offline_campaign_returning_customers b
    -- todo: adjust repalce with jinja ref
     --left join dbt_feldm.fct_campaign_returning_customers_non_offline b
     left join "airup_eu_dwh"."reports"."fct_campaign_returning_customers_non_offline" b
        on a.country = b.country
        and a.returning_customer = b.returning_customer
        and a.date = b.date