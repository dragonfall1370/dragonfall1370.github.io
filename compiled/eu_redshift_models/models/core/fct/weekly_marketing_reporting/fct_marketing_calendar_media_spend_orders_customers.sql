

WITH media_spend AS (
         SELECT media_spend_by_sales_channel_and_platform.date AS order_date,
            media_spend_by_sales_channel_and_platform.country,
            sum(media_spend_by_sales_channel_and_platform.media_spend) AS media_spend
           FROM "airup_eu_dwh"."weekly_marketing_reporting"."fct_media_spend_by_sales_channel_and_platform" media_spend_by_sales_channel_and_platform
          GROUP BY media_spend_by_sales_channel_and_platform.date, media_spend_by_sales_channel_and_platform.country
        ), orders_customers AS (
         SELECT marketing_calendar_customers_order_values.country,
            marketing_calendar_customers_order_values.order_date,
            marketing_calendar_customers_order_values.new_customer_count,
            marketing_calendar_customers_order_values.total_orders
           FROM "airup_eu_dwh"."weekly_marketing_reporting"."fct_marketing_calendar_customers_order_values" marketing_calendar_customers_order_values
          WHERE marketing_calendar_customers_order_values.data_source = 'Shopify'::text
        )
 SELECT orders_customers.country,
    orders_customers.order_date,
    orders_customers.new_customer_count,
    orders_customers.total_orders,
    COALESCE(media_spend.media_spend, 0::double precision) AS media_spend
   FROM orders_customers
     LEFT JOIN media_spend ON orders_customers.country::text = media_spend.country::text AND orders_customers.order_date = media_spend.order_date