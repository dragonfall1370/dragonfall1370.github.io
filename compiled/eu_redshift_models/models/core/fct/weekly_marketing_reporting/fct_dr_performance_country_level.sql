

WITH media_spend AS (
         SELECT media_spend_by_sales_channel_and_platform.date,
            media_spend_by_sales_channel_and_platform.country,
            COALESCE(sum(media_spend_by_sales_channel_and_platform.media_spend), 0::double precision) AS media_spend
           FROM "airup_eu_dwh"."weekly_marketing_reporting"."fct_media_spend_by_sales_channel_and_platform" media_spend_by_sales_channel_and_platform
          WHERE media_spend_by_sales_channel_and_platform.media_platform <> 'amazon'::text
          GROUP BY media_spend_by_sales_channel_and_platform.date, media_spend_by_sales_channel_and_platform.country
        ), visits_sessions AS (
         SELECT fct_shopify_visitors_conversion_rate_split_logic.day AS date,
            fct_shopify_visitors_conversion_rate_split_logic.country,
            sum(fct_shopify_visitors_conversion_rate_split_logic.sessions) AS sessions,
            sum(fct_shopify_visitors_conversion_rate_split_logic.visitors) AS visitors,
            sum(fct_shopify_visitors_conversion_rate_split_logic.orders) AS orders,
            sum(fct_shopify_visitors_conversion_rate_split_logic.add_to_card_clicks) AS add_to_card_clicks,
            sum(fct_shopify_visitors_conversion_rate_split_logic.checkout_initiation_clicks) AS checkout_initiation_clicks,
            avg(fct_shopify_visitors_conversion_rate_split_logic.bounce_rate) AS bounce_rate,
            sum(fct_shopify_visitors_conversion_rate_split_logic.pdp_views) AS pdp_views
           FROM "airup_eu_dwh"."weekly_marketing_reporting"."fct_shopify_visitors_conversion_rate_split_logic"
          GROUP BY fct_shopify_visitors_conversion_rate_split_logic.day, fct_shopify_visitors_conversion_rate_split_logic.country
        ), orders_customers AS (
         SELECT marketing_calendar_customers_order_values.order_date AS date,
            marketing_calendar_customers_order_values.country,
            sum(marketing_calendar_customers_order_values.new_customer_count) AS new_customer_count,
            sum(marketing_calendar_customers_order_values.returning_customer_count) AS returning_customer_count,
            sum(marketing_calendar_customers_order_values.total_orders) AS total_orders,
            sum(marketing_calendar_customers_order_values.total_order_value) AS total_order_value
           FROM "airup_eu_dwh"."weekly_marketing_reporting"."fct_marketing_calendar_customers_order_values" marketing_calendar_customers_order_values
          WHERE marketing_calendar_customers_order_values.data_source = 'Shopify'::text
          GROUP BY marketing_calendar_customers_order_values.order_date, marketing_calendar_customers_order_values.country
        )
 SELECT orders_customers.country,
    orders_customers.date,
    COALESCE(orders_customers.new_customer_count, 0::numeric) AS new_customers,
    COALESCE(orders_customers.total_orders, 0::double precision) AS orders,
    COALESCE(visits_sessions.sessions, 0::numeric) AS sessions,
    COALESCE(visits_sessions.visitors, 0::numeric) AS visitors,
    COALESCE(visits_sessions.add_to_card_clicks, 0::numeric) AS add_to_card_clicks,
    COALESCE(visits_sessions.checkout_initiation_clicks, 0::numeric) AS checkout_initiation_clicks,
    COALESCE(visits_sessions.bounce_rate, 0::double precision) AS bounce_rate,
    COALESCE(media_spend.media_spend, 0::double precision) AS media_spend,
    orders_customers.total_order_value,
    COALESCE(orders_customers.returning_customer_count, 0::numeric) AS returning_customers,
    COALESCE(visits_sessions.pdp_views, 0::numeric) AS pdp_views
   FROM orders_customers
     LEFT JOIN media_spend ON orders_customers.country::text = media_spend.country::text AND orders_customers.date = media_spend.date
     LEFT JOIN visits_sessions ON orders_customers.country::text = visits_sessions.country AND orders_customers.date = visits_sessions.date