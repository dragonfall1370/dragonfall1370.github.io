

WITH all_orders AS (
         SELECT order_enriched.customer_id,
            order_enriched.country_fullname AS country,
            order_enriched.created_at
           FROM "airup_eu_dwh"."shopify_global"."fct_order_enriched" order_enriched
          WHERE order_enriched.financial_status IN ('paid', 'partially_refunded')
          ORDER BY order_enriched.customer_id
        ), first_order_dates AS (
        SELECT * FROM (
            SELECT
            order_enriched.customer_id, 
            order_enriched.country_fullname AS country,
            order_enriched.created_at,
            date_trunc('day', order_enriched.created_at)::date AS cohort_day,
            row_number() OVER (PARTITION BY order_enriched.customer_id, order_enriched.country_fullname ORDER BY order_enriched.created_at) id_ranked
           FROM "airup_eu_dwh"."shopify_global"."fct_order_enriched" order_enriched
          WHERE order_enriched.financial_status IN ('paid', 'partially_refunded')
          ORDER BY order_enriched.customer_id, order_enriched.country_fullname, order_enriched.created_at ) ranked
          WHERE ranked.id_ranked = 1
        ), time_diff_from_first_order AS (
         SELECT all_orders.customer_id,
            all_orders.country,
            all_orders.created_at,
            first_order_dates.cohort_day,
            (all_orders.created_at::date - first_order_dates.created_at::date) time_from_first_order,
            rank() OVER (PARTITION BY all_orders.customer_id ORDER BY all_orders.created_at) AS reorder_rank
           FROM all_orders
             LEFT JOIN first_order_dates USING (customer_id, country)
          ORDER BY all_orders.customer_id, all_orders.country, all_orders.created_at
        ), agg_second_orders_by_days AS (
         SELECT time_diff_from_first_order.country,
            time_diff_from_first_order.time_from_first_order,
            count(time_diff_from_first_order.time_from_first_order) AS day_count,
            count(*) AS total_second_orders
           FROM time_diff_from_first_order
          WHERE time_diff_from_first_order.reorder_rank = 2
          GROUP BY time_diff_from_first_order.country, time_diff_from_first_order.time_from_first_order
          ORDER BY time_diff_from_first_order.time_from_first_order DESC
        ), grouping_days_more_than_365 AS (
         SELECT agg_second_orders_by_days.country,
                CASE
                    WHEN agg_second_orders_by_days.time_from_first_order < 365 THEN agg_second_orders_by_days.time_from_first_order
                    ELSE 365
                END AS time_bracketing,
            agg_second_orders_by_days.day_count,
            sum(agg_second_orders_by_days.total_second_orders) OVER (PARTITION BY agg_second_orders_by_days.country) AS total_second_orders
           FROM agg_second_orders_by_days
        )
 SELECT grouping_days_more_than_365.country,
    grouping_days_more_than_365.time_bracketing AS days_from_first_order,
    sum(grouping_days_more_than_365.day_count) AS day_counts,
    grouping_days_more_than_365.total_second_orders
   FROM grouping_days_more_than_365
  GROUP BY grouping_days_more_than_365.country, grouping_days_more_than_365.time_bracketing, grouping_days_more_than_365.total_second_orders