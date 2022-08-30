---Author: Abhishek Pathak
  
WITH all_orders AS (
         SELECT 
                foe.customer_id,
                foe.country_fullname AS country,
                foe.created_at,
                foe.net_revenue_2 AS revenue
         FROM 
              "airup_eu_dwh"."shopify_global"."fct_order_enriched" foe
         WHERE 
               foe.financial_status in ('paid', 'partially_refunded') ----only paying customers are considered. 
        ORDER BY foe.customer_id
        ), 
        
   first_order_dates AS (
          select customer_id,
          country,
          created_at,
          cohort_month
          from
         (SELECT 
       --      DISTINCT ON (foe.customer_id, foe.country_fullname) foe.customer_id, # Removing the distinct on clause as it is not available on redshift. Using row_number() workaround instead.
            foe.customer_id,
            foe.country_fullname AS country,
            foe.created_at,
            date_trunc('month', foe.created_at)::date AS cohort_month,
            row_number() over (PARTITION BY foe.customer_id, foe.country_fullname ORDER BY foe.customer_id, foe.country_fullname, foe.created_at) as id_ranked
           FROM 
               "airup_eu_dwh"."shopify_global"."fct_order_enriched" foe
         WHERE 
            foe.financial_status in ('paid', 'partially_refunded') ----only paying customers are considered. 
         ORDER BY 
          foe.customer_id,
           foe.country_fullname,
            foe.created_at) as ranked
         where ranked.id_ranked = 1
        ), 
        
    time_diff_from_first_order AS (
         SELECT 
                all_orders.customer_id,
                all_orders.country,
                all_orders.created_at,
                first_order_dates.cohort_month,
                all_orders.created_at - first_order_dates.created_at AS time_from_first_order,
                all_orders.revenue
         FROM 
                all_orders
         LEFT JOIN first_order_dates USING (customer_id, country)
         ORDER BY 
               all_orders.customer_id,
                all_orders.country, all_orders.created_at
        ), 
        
    rebuy_retention_counts_30_days AS (
         SELECT 
                time_diff_from_first_order.customer_id,
                time_diff_from_first_order.country,
                time_diff_from_first_order.cohort_month,
            count(time_diff_from_first_order.customer_id) OVER (PARTITION BY time_diff_from_first_order.customer_id, time_diff_from_first_order.country, time_diff_from_first_order.cohort_month) AS retention_count_30_days,
            count(*) AS rebuy_count_30_days,
            sum(time_diff_from_first_order.revenue) AS revenue_30_days
         FROM 
                time_diff_from_first_order
         WHERE 
                time_diff_from_first_order.time_from_first_order < '31 days'::interval AND time_diff_from_first_order.time_from_first_order <> '00:00:00'::interval
         GROUP BY 
                time_diff_from_first_order.customer_id,
                 time_diff_from_first_order.country,
                  time_diff_from_first_order.cohort_month
        ), 
        
    rebuy_retention_agg_30_days AS (
         SELECT 
                rebuy_retention_counts_30_days.cohort_month,
                rebuy_retention_counts_30_days.country,
            sum(rebuy_retention_counts_30_days.retention_count_30_days) AS agg_retention_count,
            sum(rebuy_retention_counts_30_days.rebuy_count_30_days) AS agg_rebuy_count,
            sum(rebuy_retention_counts_30_days.revenue_30_days) AS cumulative_revenue
         FROM 
                rebuy_retention_counts_30_days
         GROUP BY 
                rebuy_retention_counts_30_days.cohort_month,
                 rebuy_retention_counts_30_days.country
        ), 
        
    rebuy_retention_counts_60_days AS (
         SELECT 
                time_diff_from_first_order.customer_id,
                time_diff_from_first_order.country,
                time_diff_from_first_order.cohort_month,
            count(time_diff_from_first_order.customer_id) OVER (PARTITION BY time_diff_from_first_order.customer_id, time_diff_from_first_order.country, time_diff_from_first_order.cohort_month) AS retention_count_60_days,
            count(*) AS rebuy_count_60_days,
            sum(time_diff_from_first_order.revenue) AS revenue_60_days
         FROM 
                time_diff_from_first_order
         WHERE 
                time_diff_from_first_order.time_from_first_order < '61 days'::interval 
                AND time_diff_from_first_order.time_from_first_order <> '00:00:00'::interval
         GROUP BY 
                time_diff_from_first_order.customer_id,
                 time_diff_from_first_order.country,
                  time_diff_from_first_order.cohort_month
        ), 
        
    rebuy_retention_agg_60_days AS (
         SELECT 
                rebuy_retention_counts_60_days.cohort_month,
                rebuy_retention_counts_60_days.country,
            sum(rebuy_retention_counts_60_days.retention_count_60_days) AS agg_retention_count,
            sum(rebuy_retention_counts_60_days.rebuy_count_60_days) AS agg_rebuy_count,
            sum(rebuy_retention_counts_60_days.revenue_60_days) AS cumulative_revenue
         FROM 
              rebuy_retention_counts_60_days
         GROUP BY 
              rebuy_retention_counts_60_days.cohort_month,
               rebuy_retention_counts_60_days.country
        ), 
        
    rebuy_retention_counts_90_days AS (
         SELECT 
                time_diff_from_first_order.customer_id,
                time_diff_from_first_order.country,
                time_diff_from_first_order.cohort_month,
            count(time_diff_from_first_order.customer_id) OVER (PARTITION BY time_diff_from_first_order.customer_id, time_diff_from_first_order.country, time_diff_from_first_order.cohort_month) AS retention_count_90_days,
            count(*) AS rebuy_count_90_days,
            sum(time_diff_from_first_order.revenue) AS revenue_90_days
         FROM 
                time_diff_from_first_order
         WHERE 
                time_diff_from_first_order.time_from_first_order < '91 days'::interval
                AND time_diff_from_first_order.time_from_first_order <> '00:00:00'::interval
         GROUP BY 
               time_diff_from_first_order.customer_id,
                time_diff_from_first_order.country,
                 time_diff_from_first_order.cohort_month
        ), 
        
     rebuy_retention_agg_90_days AS (
         SELECT 
                rebuy_retention_counts_90_days.cohort_month,
                rebuy_retention_counts_90_days.country,
            sum(rebuy_retention_counts_90_days.retention_count_90_days) AS agg_retention_count,
            sum(rebuy_retention_counts_90_days.rebuy_count_90_days) AS agg_rebuy_count,
            sum(rebuy_retention_counts_90_days.revenue_90_days) AS cumulative_revenue
           FROM 
               rebuy_retention_counts_90_days
          GROUP BY 
                rebuy_retention_counts_90_days.cohort_month,
                 rebuy_retention_counts_90_days.country
        ), 
        
     rebuy_retention_counts_180_days AS (
         SELECT 
                time_diff_from_first_order.customer_id,
                time_diff_from_first_order.country,
                time_diff_from_first_order.cohort_month,
            count(time_diff_from_first_order.customer_id) OVER (PARTITION BY time_diff_from_first_order.customer_id, time_diff_from_first_order.country, time_diff_from_first_order.cohort_month) AS retention_count_180_days,
            count(*) AS rebuy_count_180_days,
            sum(time_diff_from_first_order.revenue) AS revenue_180_days
           FROM 
                time_diff_from_first_order
          WHERE 
                time_diff_from_first_order.time_from_first_order < '181 days'::interval 
                AND time_diff_from_first_order.time_from_first_order <> '00:00:00'::interval
          GROUP BY 
          time_diff_from_first_order.customer_id,
           time_diff_from_first_order.country,
            time_diff_from_first_order.cohort_month
        ), 
        
      rebuy_retention_agg_180_days AS (
         SELECT 
                rebuy_retention_counts_180_days.cohort_month,
                rebuy_retention_counts_180_days.country,
            sum(rebuy_retention_counts_180_days.retention_count_180_days) AS agg_retention_count,
            sum(rebuy_retention_counts_180_days.rebuy_count_180_days) AS agg_rebuy_count,
            sum(rebuy_retention_counts_180_days.revenue_180_days) AS cumulative_revenue
           FROM 
                rebuy_retention_counts_180_days
          GROUP BY 
                rebuy_retention_counts_180_days.cohort_month,
                 rebuy_retention_counts_180_days.country
        ), 
        
      rebuy_retention_counts_365_days AS (
         SELECT 
                time_diff_from_first_order.customer_id,
                time_diff_from_first_order.country,
                time_diff_from_first_order.cohort_month,
            count(time_diff_from_first_order.customer_id) OVER (PARTITION BY time_diff_from_first_order.customer_id, time_diff_from_first_order.country, time_diff_from_first_order.cohort_month) AS retention_count_365_days,
            count(*) AS rebuy_count_365_days,
            sum(time_diff_from_first_order.revenue) AS revenue_365_days
           FROM 
                time_diff_from_first_order
          WHERE 
                time_diff_from_first_order.time_from_first_order < '366 days'::interval 
                AND time_diff_from_first_order.time_from_first_order <> '00:00:00'::interval
          GROUP BY 
                time_diff_from_first_order.customer_id,
                 time_diff_from_first_order.country,
                  time_diff_from_first_order.cohort_month
        ), 
        
      rebuy_retention_agg_365_days AS (
         SELECT 
                rebuy_retention_counts_365_days.cohort_month,
                rebuy_retention_counts_365_days.country,
            sum(rebuy_retention_counts_365_days.retention_count_365_days) AS agg_retention_count,
            sum(rebuy_retention_counts_365_days.rebuy_count_365_days) AS agg_rebuy_count,
            sum(rebuy_retention_counts_365_days.revenue_365_days) AS cumulative_revenue
           FROM 
               rebuy_retention_counts_365_days
          GROUP BY 
                rebuy_retention_counts_365_days.cohort_month,
                 rebuy_retention_counts_365_days.country
        ), 
        
      rebuy_retention_counts_all_time AS (
         SELECT 
                time_diff_from_first_order.customer_id,
                time_diff_from_first_order.country,
                time_diff_from_first_order.cohort_month,
            count(time_diff_from_first_order.customer_id) OVER (PARTITION BY time_diff_from_first_order.customer_id, time_diff_from_first_order.country, time_diff_from_first_order.cohort_month) AS retention_count_all_time,
            count(*) AS rebuy_count_all_time,
            sum(time_diff_from_first_order.revenue) AS revenue_all_time
           FROM 
                time_diff_from_first_order
          WHERE 
                time_diff_from_first_order.time_from_first_order <> '00:00:00'::interval
          GROUP BY 
                time_diff_from_first_order.customer_id,
                 time_diff_from_first_order.country,
                  time_diff_from_first_order.cohort_month
        ), 
        
      rebuy_retention_agg_all_time AS (
         SELECT 
                rebuy_retention_counts_all_time.cohort_month,
                rebuy_retention_counts_all_time.country,
            sum(rebuy_retention_counts_all_time.retention_count_all_time) AS agg_retention_count,
            sum(rebuy_retention_counts_all_time.rebuy_count_all_time) AS agg_rebuy_count,
            sum(rebuy_retention_counts_all_time.revenue_all_time) AS cumulative_revenue
           FROM 
                rebuy_retention_counts_all_time
          GROUP BY 
                rebuy_retention_counts_all_time.cohort_month,
                 rebuy_retention_counts_all_time.country
        ), 
        
      cohort_size_counts AS (
         SELECT 
                time_diff_from_first_order.country,
                time_diff_from_first_order.cohort_month,
            count(*) AS cohort_size
           FROM 
                time_diff_from_first_order
          GROUP BY
                time_diff_from_first_order.country,
                 time_diff_from_first_order.cohort_month
        ), 
        
      retention_rebuy_revenue_segment AS (
      SELECT 
            cohort_size_counts.cohort_month,
            cohort_size_counts.country,
            cohort_size_counts.cohort_size,
            rebuy_retention_agg_30_days.agg_retention_count / cohort_size_counts.cohort_size::numeric AS retention_rate_30_days,
            rebuy_retention_agg_60_days.agg_retention_count / cohort_size_counts.cohort_size::numeric AS retention_rate_60_days,
            rebuy_retention_agg_90_days.agg_retention_count / cohort_size_counts.cohort_size::numeric AS retention_rate_90_days,
            rebuy_retention_agg_180_days.agg_retention_count / cohort_size_counts.cohort_size::numeric AS retention_rate_180_days,
            rebuy_retention_agg_365_days.agg_retention_count / cohort_size_counts.cohort_size::numeric AS retention_rate_365_days,
            rebuy_retention_agg_all_time.agg_retention_count / cohort_size_counts.cohort_size::numeric AS retention_rate_all_time,
            rebuy_retention_agg_30_days.agg_rebuy_count / cohort_size_counts.cohort_size::numeric AS rebuy_rate_30_days,
            rebuy_retention_agg_60_days.agg_rebuy_count / cohort_size_counts.cohort_size::numeric AS rebuy_rate_60_days,
            rebuy_retention_agg_90_days.agg_rebuy_count / cohort_size_counts.cohort_size::numeric AS rebuy_rate_90_days,
            rebuy_retention_agg_180_days.agg_rebuy_count / cohort_size_counts.cohort_size::numeric AS rebuy_rate_180_days,
            rebuy_retention_agg_365_days.agg_rebuy_count / cohort_size_counts.cohort_size::numeric AS rebuy_rate_365_days,
            rebuy_retention_agg_all_time.agg_rebuy_count / cohort_size_counts.cohort_size::numeric AS rebuy_rate_all_time,
            rebuy_retention_agg_30_days.cumulative_revenue AS cumulative_revenue_30_days,
            rebuy_retention_agg_60_days.cumulative_revenue AS cumulative_revenue_60_days,
            rebuy_retention_agg_90_days.cumulative_revenue AS cumulative_revenue_90_days,
            rebuy_retention_agg_180_days.cumulative_revenue AS cumulative_revenue_180_days,
            rebuy_retention_agg_365_days.cumulative_revenue AS cumulative_revenue_365_days,
            rebuy_retention_agg_all_time.cumulative_revenue AS cumulative_revenue_all_time
        FROM cohort_size_counts
             JOIN rebuy_retention_agg_30_days USING (cohort_month, country)
             JOIN rebuy_retention_agg_60_days USING (cohort_month, country)
             JOIN rebuy_retention_agg_90_days USING (cohort_month, country)
             JOIN rebuy_retention_agg_180_days USING (cohort_month, country)
             JOIN rebuy_retention_agg_365_days USING (cohort_month, country)
             JOIN rebuy_retention_agg_all_time USING (cohort_month, country)
        )
 SELECT 
            retention_rebuy_revenue_segment.cohort_month,
            retention_rebuy_revenue_segment.country,
            retention_rebuy_revenue_segment.retention_rate_30_days,
            retention_rebuy_revenue_segment.retention_rate_60_days,
            retention_rebuy_revenue_segment.retention_rate_90_days,
            retention_rebuy_revenue_segment.retention_rate_180_days,
            retention_rebuy_revenue_segment.retention_rate_365_days,
            retention_rebuy_revenue_segment.retention_rate_all_time,
            retention_rebuy_revenue_segment.rebuy_rate_30_days,
            retention_rebuy_revenue_segment.rebuy_rate_60_days,
            retention_rebuy_revenue_segment.rebuy_rate_90_days,
            retention_rebuy_revenue_segment.rebuy_rate_180_days,
            retention_rebuy_revenue_segment.rebuy_rate_365_days,
            retention_rebuy_revenue_segment.rebuy_rate_all_time,
            retention_rebuy_revenue_segment.cumulative_revenue_30_days,
            retention_rebuy_revenue_segment.cumulative_revenue_60_days,
            retention_rebuy_revenue_segment.cumulative_revenue_90_days,
            retention_rebuy_revenue_segment.cumulative_revenue_180_days,
            retention_rebuy_revenue_segment.cumulative_revenue_365_days,
            retention_rebuy_revenue_segment.cumulative_revenue_all_time,
            retention_rebuy_revenue_segment.cohort_size
   FROM 
            retention_rebuy_revenue_segment