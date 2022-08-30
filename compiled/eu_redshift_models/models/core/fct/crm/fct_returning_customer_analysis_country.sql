---Author: Abhishek Pathak
  

WITH initial_orders AS (
        select customer_id,
                country,
                init_order_timestamp,
                init_order_cohort_month,
                init_order_number
            from(
         SELECT order_enriched.customer_id, -- DISTINCT ON (order_enriched.customer_id, order_enriched.country_fullname) order_enriched.customer_id,
            order_enriched.country_fullname AS country,
            order_enriched.created_at AS init_order_timestamp,
            date_trunc('month', order_enriched.created_at) AS init_order_cohort_month,
            order_enriched.order_number AS init_order_number,
            row_number() over (PARTITION BY customer_id, country_fullname ORDER BY customer_id, country_fullname, created_at) as id_ranked
           FROM "airup_eu_dwh"."shopify_global"."fct_order_enriched" order_enriched
          WHERE order_enriched.financial_status in ('paid', 'partially_refunded')
          ORDER BY order_enriched.customer_id, order_enriched.country_fullname, order_enriched.created_at, order_enriched.order_number) as ranked
        where ranked.id_ranked = 1
        ), distinct_orders AS (
         SELECT DISTINCT order_enriched.order_number,
            order_enriched.country_fullname AS country,
            order_enriched.created_at,
            sum(order_enriched.net_revenue_2) AS net_revenue
           FROM "airup_eu_dwh"."shopify_global"."fct_order_enriched" order_enriched
             LEFT JOIN "airup_eu_dwh"."shopify_global"."fct_order_shipping_line" order_shipping_line ON order_enriched.id = order_shipping_line.order_id
          WHERE order_enriched.financial_status in ('paid', 'partially_refunded')
          GROUP BY order_enriched.order_number, order_enriched.country_fullname, order_enriched.created_at
        )
 SELECT
        CASE
            WHEN initial_orders.init_order_number IS NULL THEN 'Returning D2C Customer'::text
            ELSE 'New D2C Customer'::text
        END AS returning_customer_flag,
    distinct_orders.country,
    date(date_trunc('month'::text, distinct_orders.created_at)) AS month_classification,
    date(distinct_orders.created_at) AS order_date,
    count(DISTINCT distinct_orders.order_number) AS orders,
    sum(distinct_orders.net_revenue) AS net_revenue
   FROM distinct_orders
     LEFT JOIN initial_orders ON distinct_orders.order_number = initial_orders.init_order_number
  GROUP BY (
        CASE
            WHEN initial_orders.init_order_number IS NULL THEN 'Returning D2C Customer'::text
            ELSE 'New D2C Customer'::text
        END), distinct_orders.country, (date_trunc('month'::text, distinct_orders.created_at)), (date(distinct_orders.created_at))