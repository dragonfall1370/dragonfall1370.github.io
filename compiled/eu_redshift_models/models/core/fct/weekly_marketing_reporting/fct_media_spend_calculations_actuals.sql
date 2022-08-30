

WITH orders_new_customers AS (
         SELECT date_trunc('month'::text, marketing_calendar_customers_order_values.order_date::timestamp with time zone)::date AS reported_month,
            sum(marketing_calendar_customers_order_values.new_customer_count) AS new_customer_count,
            sum(marketing_calendar_customers_order_values.total_orders) AS total_orders,
            country_system_account_mapping.country_grouping AS region
           FROM "airup_eu_dwh"."weekly_marketing_reporting"."fct_marketing_calendar_customers_order_values" marketing_calendar_customers_order_values
             JOIN "airup_eu_dwh"."public"."country_system_account_mapping" country_system_account_mapping ON marketing_calendar_customers_order_values.country::text = country_system_account_mapping.country_fullname::text
          GROUP BY (date_trunc('month'::text, marketing_calendar_customers_order_values.order_date::timestamp with time zone)), country_system_account_mapping.country_grouping
        ), media_spend_actuals AS (
         SELECT marketing_spend.reported_month,
            marketing_spend.region,
            sum(marketing_spend.amount) AS media_spend
           FROM weekly_marketing_reporting.marketing_spend
          GROUP BY marketing_spend.reported_month, marketing_spend.region
        )
 SELECT orders_new_customers.reported_month,
    media_spend_actuals.region,
    orders_new_customers.new_customer_count,
    orders_new_customers.total_orders,
    media_spend_actuals.media_spend
   FROM orders_new_customers
     JOIN media_spend_actuals ON orders_new_customers.reported_month = media_spend_actuals.reported_month AND
        CASE
            WHEN media_spend_actuals.region::text = 'North EU'::text THEN 'North Europe'::text
            WHEN media_spend_actuals.region::text = 'Central EU'::text THEN 'Central Europe'::text
            WHEN media_spend_actuals.region::text = 'South EU'::text THEN 'South Europe'::text
            ELSE NULL::text
        END = orders_new_customers.region::text