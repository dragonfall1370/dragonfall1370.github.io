---legacy: crm.cohort_retention_curve_country

WITH cohort_items AS (
         SELECT 
            date_trunc('month', foe.created_at)::date AS cohort_month,
            foe.created_at AS order_date,
            foe.customer_id,
            foe.country_fullname AS country
           FROM "airup_eu_dwh"."shopify_global"."fct_order_enriched" foe
          ORDER BY 
             (date_trunc('month', foe.created_at)::date),
              foe.created_at
        ), 
        user_activities AS (
         SELECT foe.customer_id,
            cohort_items.country,
            (date_part('year', cohort_items.cohort_month) - date_part('year', '2019-08-02'::date)) * 12::double precision + (date_part('month', cohort_items.cohort_month) - date_part('month', '2019-08-02'::date)) AS month_number
           FROM "airup_eu_dwh"."shopify_global"."fct_order_enriched" foe
             LEFT JOIN cohort_items ON foe.customer_id = cohort_items.customer_id
          GROUP BY 
          foe.customer_id,
           cohort_items.country,
            ((date_part('year', cohort_items.cohort_month) - date_part('year', '2019-08-02'::date))
             * 12 +
             (date_part('month', cohort_items.cohort_month) 
             - date_part('month', '2019-08-02'::date))),
              cohort_items.cohort_month
        ), 
        
        cohort_size AS (
         SELECT cohort_items.cohort_month,
            cohort_items.country,
            count(*) AS num_users
           FROM cohort_items
          GROUP BY 
          cohort_items.cohort_month,
           cohort_items.country
          ORDER BY 
          cohort_items.country,
           cohort_items.cohort_month
        ), 
        
        retention_table AS (
         SELECT cohort_items.cohort_month,
            user_activities.month_number,
            cohort_items.country,
            count(*) AS num_users
           FROM user_activities
             LEFT JOIN cohort_items ON user_activities.customer_id = cohort_items.customer_id
          GROUP BY 
          cohort_items.cohort_month,
           user_activities.month_number,
            cohort_items.country
        ), 
        
        cohort_chart AS (
         SELECT retention_table.cohort_month,
            retention_table.month_number,
            retention_table.country,
            retention_table.num_users,
            cohort_size.num_users AS total_users,
            retention_table.num_users::double precision / cohort_size.num_users::double precision AS retention_pct
           FROM retention_table
             LEFT JOIN cohort_size ON retention_table.cohort_month = cohort_size.cohort_month AND retention_table.country::text = cohort_size.country::text
          WHERE retention_table.cohort_month IS NOT NULL
          ORDER BY 
          retention_table.country,
           retention_table.cohort_month,
            retention_table.month_number
        ), 
        
        distinct_cohort_month AS (
         SELECT DISTINCT cohort_chart.cohort_month,
            dense_rank() OVER (PARTITION BY cohort_chart.country ORDER BY cohort_chart.country, cohort_chart.cohort_month) AS rank1,
                CASE
                    WHEN dense_rank() OVER (PARTITION BY cohort_chart.country ORDER BY cohort_chart.country, cohort_chart.cohort_month) = dense_rank() OVER (PARTITION BY cohort_chart.country ORDER BY cohort_chart.country, cohort_chart.cohort_month) THEN dense_rank() OVER (PARTITION BY cohort_chart.country ORDER BY cohort_chart.country, cohort_chart.cohort_month) - 1
                    ELSE NULL::bigint
                END AS month_number,
            cohort_chart.country
           FROM cohort_chart
          ORDER BY 
          cohort_chart.country,
           cohort_chart.cohort_month,
            (dense_rank() OVER (PARTITION BY cohort_chart.country ORDER BY cohort_chart.country, cohort_chart.cohort_month))
        ), 
        
        retention_average AS (
         SELECT cohort_chart.month_number,
            cohort_chart.country,
                CASE
                    WHEN cohort_chart.month_number = cohort_chart.month_number THEN ('Month' || ' '::text) || cohort_chart.month_number
                    ELSE NULL::text
                END AS month_nos,
            avg(cohort_chart.retention_pct) AS avg_retention_pct
           FROM cohort_chart
          GROUP BY
           cohort_chart.month_number,
            cohort_chart.country, (
                CASE
                    WHEN cohort_chart.month_number = cohort_chart.month_number THEN ('Month' || ' '::text) || cohort_chart.month_number
                    ELSE NULL::text
                END)
          ORDER BY 
          cohort_chart.country,
           cohort_chart.month_number
        )
 SELECT 
    distinct_cohort_month.cohort_month,
    distinct_cohort_month.country,
    retention_average.month_number,
    retention_average.avg_retention_pct
   FROM 
     retention_average
     JOIN distinct_cohort_month
     USING (country, month_number)