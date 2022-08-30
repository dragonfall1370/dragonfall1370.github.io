--legacy: no longer used; rollup has to be replace by unions if ever needed 



WITH all_orders_per_customer AS (
    SELECT DISTINCT foe.customer_id,
                    foe.order_number,
                    foe.created_at,
                    mapping.subcategory_3_clean as pod_flavour,
                    CASE
                        WHEN cppfm.starter_set_flag::text = 'starter-set'::text THEN 1
                        ELSE 0
                        END                                                                                    AS starter_sets,
                    dense_rank()
                    OVER (PARTITION BY foe.customer_id ORDER BY foe.created_at, foe.order_number ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)              AS nth_order,
                    min(foe.created_at)
                    OVER (PARTITION BY foe.customer_id ORDER BY foe.created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)                                AS customer_min_created_at
    FROM "airup_eu_dwh"."shopify_global"."fct_order_enriched" foe
             LEFT JOIN "airup_eu_dwh"."shopify_global"."fct_order_line" fol ON foe.id = fol.order_id
             left outer join "airup_eu_dwh"."shopify_global"."shopify_product_categorisation" mapping
                on mapping.SKU = fol.SKU
),
     returning_customer_classification AS (
         SELECT all_orders_per_customer.customer_id,
                all_orders_per_customer.order_number,
                all_orders_per_customer.created_at,
                all_orders_per_customer.customer_min_created_at,
                all_orders_per_customer.nth_order,
                all_orders_per_customer.line_items_title,
                all_orders_per_customer.pod_flavour,
                all_orders_per_customer.starter_set_flag,
                max(all_orders_per_customer.starter_sets)
                OVER (PARTITION BY all_orders_per_customer.order_number)                                           AS order_incl_starter_set,
                max(all_orders_per_customer.nth_order) OVER (PARTITION BY all_orders_per_customer.customer_id) >
                1                                                                                                  AS returning_customer,
                CASE
                    WHEN all_orders_per_customer.nth_order > 1 THEN all_orders_per_customer.created_at -
                                                                    all_orders_per_customer.customer_min_created_at
                    ELSE NULL::interval
                    END                                                                                            AS "case",
                max(
                CASE
                    WHEN all_orders_per_customer.nth_order = 2 AND
                         (all_orders_per_customer.created_at - all_orders_per_customer.customer_min_created_at) <=
                         '7 days'::interval THEN 1
                    ELSE NULL::integer
                    END)
                OVER (PARTITION BY all_orders_per_customer.customer_id)                                            AS returning_customer_7_days,
                max(
                CASE
                    WHEN all_orders_per_customer.nth_order = 2 AND
                         (all_orders_per_customer.created_at - all_orders_per_customer.customer_min_created_at) <=
                         '30 days'::interval THEN 1
                    ELSE NULL::integer
                    END)
                OVER (PARTITION BY all_orders_per_customer.customer_id)                                            AS returning_customer_30_days,
                max(
                CASE
                    WHEN all_orders_per_customer.nth_order = 2 AND
                         (all_orders_per_customer.created_at - all_orders_per_customer.customer_min_created_at) <=
                         '90 days'::interval THEN 1
                    ELSE NULL::integer
                    END)
                OVER (PARTITION BY all_orders_per_customer.customer_id)                                            AS returning_customer_90_days
         FROM all_orders_per_customer
     )
SELECT returning_customer_classification.pod_flavour,
       returning_customer_classification.order_incl_starter_set = 1  AS init_order_starter_set,
       count(DISTINCT returning_customer_classification.customer_id) AS customers,
       count(DISTINCT
             CASE
                 WHEN returning_customer_classification.returning_customer
                     THEN returning_customer_classification.customer_id
                 ELSE NULL::text
                 END)                                                AS returning_customers,
       count(DISTINCT
             CASE
                 WHEN returning_customer_classification.returning_customer_7_days = 1
                     THEN returning_customer_classification.customer_id
                 ELSE NULL::text
                 END)                                                AS returning_customers_7_days,
       count(DISTINCT
             CASE
                 WHEN returning_customer_classification.returning_customer_30_days = 1
                     THEN returning_customer_classification.customer_id
                 ELSE NULL::text
                 END)                                                AS returning_customers_30_days,
       count(DISTINCT
             CASE
                 WHEN returning_customer_classification.returning_customer_90_days = 1
                     THEN returning_customer_classification.customer_id
                 ELSE NULL::text
                 END)                                                AS returning_customers_90_days,
       COALESCE(returning_customer_classification.starter_set_flag,
                CASE
                    WHEN (GROUPING(returning_customer_classification.starter_set_flag)) = 0 THEN 'unknown'::text
                    ELSE 'all'::text
                    END::character varying)                          AS starter_set_flag
FROM returning_customer_classification
WHERE returning_customer_classification.nth_order = 1
GROUP BY returning_customer_classification.pod_flavour, ROLLUP ( returning_customer_classification.starter_set_flag),
         (returning_customer_classification.order_incl_starter_set = 1);