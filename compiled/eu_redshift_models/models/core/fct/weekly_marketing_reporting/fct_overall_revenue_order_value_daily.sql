

with
unionized_data as
    (select * from "airup_eu_dwh"."shopify_global"."fct_daily_net_revenue_orders_by_country"
    union all
    select * from "airup_eu_dwh"."amazon"."fct_amz_daily_net_revenue_orders_by_country")

-- ####################################
-- MAIN QUERY
-- ####################################
        
select
    sales_channel,
    shipping_country,
    country_grouping,
    "date",
    gross_revenue,
    net_revenue_1,
    net_revenue_2,
    net_orders,
    gross_orders,
    net_volume
from
    unionized_data