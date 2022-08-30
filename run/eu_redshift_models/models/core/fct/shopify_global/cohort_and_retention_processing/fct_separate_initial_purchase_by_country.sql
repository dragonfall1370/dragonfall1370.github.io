

  create view "airup_eu_dwh"."shopify_global"."fct_separate_initial_purchase_by_country__dbt_tmp" as (
     
WITH product_categories_per_order AS (
         SELECT 
            foe.order_number AS oid,
            -- To Do: replace inline mapping with product categorization mapping table
            sum(
                CASE WHEN lower(dpe.product_type) in ('starter kit', 'starter-set') THEN fol.quantity ELSE NULL END) AS starter_sets,
            sum(
                CASE WHEN lower(dpe.product_type::text) in  ('pod', 'aromapod', 'aromapod-bundle', 'aromapod-bundle-mix') THEN fol.quantity ELSE NULL END) AS pods,
            sum(
                CASE WHEN lower(dpe.product_type::text) = 'accessories'::text THEN fol.quantity ELSE NULL END) AS loops
           FROM "airup_eu_dwh"."shopify_global"."fct_order_enriched" foe
             LEFT JOIN "airup_eu_dwh"."shopify_global"."fct_order_line" fol ON foe.id = fol.order_id
             LEFT JOIN "airup_eu_dwh"."shopify_global"."dim_product_enriched" dpe ON fol.product_id = dpe.id
          GROUP BY 
             foe.order_number
        ),

      data_prep_by_country AS (
         SELECT DISTINCT 
            foe.country_abbreviation,
            foe.country_fullname,
            foe.customer_id,
            foe.order_number,
            (sum(1) OVER (PARTITION BY foe.customer_id, foe.country_abbreviation ORDER BY foe.created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS nth_order,
            foe.created_at,
                CASE
                    ---WHEN (sum(1) OVER (PARTITION BY foe.customer_id, foe.country_abbreviation ORDER BY foe.created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) = 1 
                    WHEN (sum(1) OVER (PARTITION BY foe.customer_id, foe.country_abbreviation ORDER BY foe.created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) = 1 
                    AND product_categories_per_order.starter_sets >= 1 
                    THEN 1
                    ELSE 0
                END AS init_order_starter_set,
                CASE
                    --WHEN (sum(1) OVER (PARTITION BY foe.customer_id, foe.country_abbreviation ORDER BY foe.created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) = 1 
                    WHEN (sum(1) OVER (PARTITION BY foe.customer_id, foe.country_abbreviation ORDER BY foe.created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) = 1 
                    AND product_categories_per_order.pods >= 1 
                    THEN 1
                    ELSE 0
                END AS init_order_pods,
            ---lead(foe.created_at) OVER (PARTITION BY foe.customer_id, foe.country_abbreviation ORDER BY foe.created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS created_at_next_order,
            lead(foe.created_at) OVER (PARTITION BY foe.customer_id, foe.country_abbreviation ORDER BY foe.created_at) AS created_at_next_order,
            lead(foe.created_at) OVER (PARTITION BY foe.customer_id, foe.country_abbreviation ORDER BY foe.created_at) - foe.created_at AS time_diff_next_order,
            min(date(date_trunc('month'::text, foe.created_at))) OVER (PARTITION BY foe.customer_id, foe.country_abbreviation) AS cohort,
            date_trunc('month'::text, foe.created_at) AS month_created_at,
            -- ##############################
		        -- calculate date diff in months
            (date_part('year', foe.created_at::date) 
            - date_part('year', min(date_trunc('month', foe.created_at)) OVER (PARTITION BY foe.customer_id)::date))
             * 12 +
            (date_part('month', foe.created_at::date)
            - date_part('month', min(date_trunc('month'::text, foe.created_at)) OVER (PARTITION BY foe.customer_id)::date)) AS date_diff_months,
            sum(foe.net_revenue_2) AS net_revenue,
            product_categories_per_order.starter_sets,
            product_categories_per_order.pods,
            product_categories_per_order.loops,
            product_categories_per_order.pods >= 1 AS pod_order
           FROM 
                "airup_eu_dwh"."shopify_global"."fct_order_enriched" foe
             LEFT JOIN product_categories_per_order
            ON foe.order_number = product_categories_per_order.oid
           WHERE foe.financial_status in ('paid', 'partially_refunded')
          GROUP BY 
          foe.customer_id,
           foe.country_abbreviation,
            foe.country_fullname,
             foe.order_number,
              foe.created_at,
               product_categories_per_order.starter_sets,
                product_categories_per_order.pods,
                 product_categories_per_order.loops
          ORDER BY 
          foe.customer_id,
           foe.created_at
        ),
      data_prep_2_by_country AS (
         SELECT country_abbreviation,
                country_fullname,
                customer_id,
                order_number,
                nth_order,
                created_at,
                init_order_starter_set,
                init_order_pods,
                created_at_next_order,
                time_diff_next_order,
                cohort,
                month_created_at,
          CASE
            WHEN nth_order = 1 
            THEN '-1'::integer
            ELSE date_diff_months
          END AS date_diff_months,
            net_revenue,
            starter_sets,
            pods,
            loops,
            (sum(1) OVER (PARTITION BY customer_id, country_abbreviation ORDER BY created_at, order_number ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS order_per_client,
                CASE
                    WHEN max(init_order_starter_set) OVER (PARTITION BY customer_id, country_abbreviation) = 1 
                    THEN 'yes'::text
                    ELSE 'no'::text
                END AS init_order_incl_starter_set,
                CASE
                    WHEN max(init_order_pods) OVER (PARTITION BY customer_id, country_abbreviation) = 1 
                    THEN 'yes'::text
                    ELSE 'no'::text
                END AS init_order_incl_pods,
            sum(pods) OVER (PARTITION BY customer_id, country_abbreviation) AS pod_items_per_customer,
            count(pods) OVER (PARTITION BY customer_id, country_abbreviation, date_diff_months) AS pod_orders_per_customer
           FROM data_prep_by_country
        ),
        data_prep_3_by_country AS (
         SELECT country_abbreviation,
                country_fullname,
                customer_id,
                cohort,
                date_diff_months,
            max(init_order_incl_starter_set) AS init_order_incl_starter_set,
            max(init_order_incl_pods) AS init_order_incl_pods,
            sum(net_revenue) AS net_revenue,
            max(pod_items_per_customer) AS pod_items_per_customer,
            max(pod_orders_per_customer) AS pod_orders_per_customer,
            avg(time_diff_next_order) AS time_diff_next_order,
            count(DISTINCT
          CASE
            WHEN order_per_client > 1 THEN order_number
              ELSE NULL::text
          END) AS subsequent_orders,
            count(DISTINCT
                CASE
                    WHEN order_per_client > 1 AND pods >= 1 THEN order_number
                    ELSE NULL::text
                END) AS subsequent_orders_with_pods,
            count(DISTINCT
                CASE
                    WHEN order_per_client > 1 AND loops >= 1 THEN order_number
                    ELSE NULL::text
                END) AS subsequent_orders_with_loops,
            count(DISTINCT
                CASE
                    WHEN order_per_client > 1 AND (pods >= 1 OR loops >= 1) THEN order_number
                    ELSE NULL::text
                END) AS subsequent_orders_with_pods_or_loops,
            count(DISTINCT
                CASE
                    WHEN order_per_client > 1 OR order_per_client = 1 AND pods >= 1 THEN order_number
                    ELSE NULL::text
                END) AS subsequent_orders_pods_first,
            count(DISTINCT
                CASE
                    WHEN order_per_client > 0 AND pods >= 1 THEN order_number
                    ELSE NULL::text
                END) AS subsequent_orders_with_pods_pods_first,
            count(DISTINCT
                CASE
                    WHEN order_per_client > 0 AND loops >= 1 THEN order_number
                    ELSE NULL::text
                END) AS subsequent_orders_with_loops_pods_first,
            count(DISTINCT
                CASE
                    WHEN order_per_client > 0 AND (pods >= 1 OR loops >= 1) THEN order_number
                    ELSE NULL::text
                END) AS subsequent_orders_with_pods_or_loops_pods_first
           FROM data_prep_2_by_country
          GROUP BY country_abbreviation, country_fullname, customer_id, cohort, date_diff_months
        ),
     returning_customers_by_country AS (
         SELECT country_abbreviation,
            country_fullname,
            cohort,
            init_order_incl_starter_set,
            init_order_incl_pods,
            count(DISTINCT
                CASE
                    WHEN order_per_client > 1 THEN customer_id
                    ELSE NULL::text
                END) AS returning_customers,
            count(DISTINCT
                CASE
                    WHEN order_per_client > 1 OR order_per_client = 1 AND pods >= 1 THEN customer_id
                    ELSE NULL::text
                END) AS returning_customers_pods_first
           FROM data_prep_2_by_country
          GROUP BY country_abbreviation, country_fullname, cohort, init_order_incl_starter_set, init_order_incl_pods
        )
      SELECT data_prep_3_by_country.country_abbreviation,
    data_prep_3_by_country.country_fullname,
    data_prep_3_by_country.cohort,
    data_prep_3_by_country.date_diff_months,
    data_prep_3_by_country.init_order_incl_starter_set,
    data_prep_3_by_country.init_order_incl_pods,
    sum(data_prep_3_by_country.net_revenue) AS net_revenue,
    sum(data_prep_3_by_country.pod_items_per_customer) / NULLIF(count(DISTINCT data_prep_3_by_country.customer_id), 0) AS avg_pod_items_per_customer,
    sum(data_prep_3_by_country.pod_orders_per_customer) / NULLIF(count(DISTINCT data_prep_3_by_country.customer_id), 0)::numeric AS avg_pod_orders_per_customer,
    count(DISTINCT data_prep_3_by_country.customer_id) AS nr_of_customers,
    extract('epoch' from avg(data_prep_3_by_country.time_diff_next_order))/ 86400 AS avg_order_date_diff_days,
    ---date_part( 'epoch' , avg(data_prep_3_by_country.time_diff_next_order)) / 86400 AS avg_order_date_diff_days,
    sum(data_prep_3_by_country.subsequent_orders) AS subsequent_orders,
    sum(data_prep_3_by_country.pod_orders_per_customer) AS pod_orders_per_customer,
    returning_customers_by_country.returning_customers AS returning_customers_cohort,
    returning_customers_by_country.returning_customers_pods_first AS returning_customers_pods_first_cohort,
    sum(data_prep_3_by_country.subsequent_orders_with_pods) AS subsequent_orders_with_pods,
    sum(data_prep_3_by_country.subsequent_orders_with_loops) AS subsequent_orders_with_loops,
    sum(data_prep_3_by_country.subsequent_orders_pods_first) AS subsequent_orders_pods_first,
    sum(data_prep_3_by_country.subsequent_orders_with_pods_or_loops) AS subsequent_orders_with_pods_or_loops,
    sum(data_prep_3_by_country.subsequent_orders_with_pods_pods_first) AS subsequent_orders_with_pods_pods_first,
    sum(data_prep_3_by_country.subsequent_orders_with_loops_pods_first) AS subsequent_orders_with_loops_pods_first,
    sum(data_prep_3_by_country.subsequent_orders_with_pods_or_loops_pods_first) AS subsequent_orders_with_pods_or_loops_pods_first
   FROM data_prep_3_by_country
     LEFT JOIN returning_customers_by_country ON data_prep_3_by_country.country_abbreviation::text = returning_customers_by_country.country_abbreviation::text AND data_prep_3_by_country.country_fullname::text = returning_customers_by_country.country_fullname::text AND data_prep_3_by_country.cohort = returning_customers_by_country.cohort AND data_prep_3_by_country.init_order_incl_starter_set = returning_customers_by_country.init_order_incl_starter_set AND data_prep_3_by_country.init_order_incl_pods = returning_customers_by_country.init_order_incl_pods
  GROUP BY 
  data_prep_3_by_country.country_abbreviation,
   data_prep_3_by_country.country_fullname,
    data_prep_3_by_country.cohort,
     data_prep_3_by_country.date_diff_months,
      data_prep_3_by_country.init_order_incl_starter_set,
       data_prep_3_by_country.init_order_incl_pods,
        returning_customers_by_country.returning_customers,
         returning_customers_by_country.returning_customers_pods_first
  ) with no schema binding;
