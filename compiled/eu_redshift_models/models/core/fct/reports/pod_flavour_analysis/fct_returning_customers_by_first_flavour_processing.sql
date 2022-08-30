

--### gathering all customer orders
--### & mapping flavours & starter-sets
--### filtering out accessories, unpaid orders, etc.
WITH all_orders_per_customer AS (
    SELECT order_enriched.customer_id,
           order_line.order_id,
           order_line.title,
           order_enriched.created_at,
           order_enriched.country_abbreviation,
		   case
		       when seed_flavours_ss.subcategory_4 is null then seed_flavours.subcategory_3_clean
		       when seed_flavours_ss.subcategory_4 is not null then seed_flavours_ss.subcategory_4
		       else 'Unmapped Flavour'
		   end as pod_flavour,
           case when seed_flavours.category = 'Hardware' then 'starter-set' else 'pod' end as starter_set_flag
     FROM "airup_eu_dwh"."shopify_global"."fct_order_enriched" order_enriched
--	FROM shopify_global.fct_order_enriched order_enriched
--             LEFT JOIN shopify_global.fct_order_line order_line
 			 LEFT JOIN "airup_eu_dwh"."shopify_global"."fct_order_line" order_line
                ON order_enriched.id = order_line.order_id
--        	 LEFT JOIN shopify_global.seed_shopify_product_categorisation seed_flavours
 			 LEFT JOIN "airup_eu_dwh"."shopify_global"."shopify_product_categorisation" seed_flavours
        	    ON order_line.sku = seed_flavours.sku
--        	 LEFT JOIN shopify_global.seed_shopify_product_categorisation_ss_and_bundle_content seed_flavours_ss
 			 LEFT JOIN "airup_eu_dwh"."shopify_global"."seed_shopify_product_categorisation_ss_and_bundle_content" seed_flavours_ss
        		ON seed_flavours.sku = seed_flavours_ss.sku
    WHERE
    	seed_flavours.category in ('Flavour', 'Hardware')
    	and order_enriched.financial_status::text in ('paid', 'partially_refunded')
		),

		--### identification min order data
		--### per customer & per flavour

     first_orders_per_flavour AS (
         SELECT all_orders_per_customer.customer_id,
                all_orders_per_customer.pod_flavour     AS pod_flavour_first,
                min(all_orders_per_customer.created_at) AS min_order_date_per_customer_per_flavour
         FROM all_orders_per_customer
         GROUP BY all_orders_per_customer.customer_id, all_orders_per_customer.pod_flavour
     ),

     first_orders_per_flavour_enhanced AS (
         SELECT all_orders_per_customer.customer_id,
                all_orders_per_customer.country_abbreviation,
                all_orders_per_customer.order_id,
                all_orders_per_customer.starter_set_flag,
                first_orders_per_flavour.pod_flavour_first,
                first_orders_per_flavour.min_order_date_per_customer_per_flavour
         FROM all_orders_per_customer
                  LEFT JOIN first_orders_per_flavour
                            ON first_orders_per_flavour.customer_id = all_orders_per_customer.customer_id AND
                               first_orders_per_flavour.pod_flavour_first = all_orders_per_customer.pod_flavour
         WHERE all_orders_per_customer.created_at = first_orders_per_flavour.min_order_date_per_customer_per_flavour
         GROUP BY all_orders_per_customer.customer_id, all_orders_per_customer.country_abbreviation,
                  all_orders_per_customer.order_id, all_orders_per_customer.starter_set_flag,
                  first_orders_per_flavour.pod_flavour_first,
                  first_orders_per_flavour.min_order_date_per_customer_per_flavour
     ),
     all_orders_per_customer_enhanced AS (
         SELECT all_orders_per_customer.customer_id,
                all_orders_per_customer.order_id,
                all_orders_per_customer.created_at,
                first_orders_per_flavour_enhanced.pod_flavour_first,
                first_orders_per_flavour_enhanced.min_order_date_per_customer_per_flavour,
                first_orders_per_flavour_enhanced.country_abbreviation,
                first_orders_per_flavour_enhanced.starter_set_flag,
                dense_rank()
                OVER (PARTITION BY first_orders_per_flavour_enhanced.customer_id, first_orders_per_flavour_enhanced.pod_flavour_first, first_orders_per_flavour_enhanced.country_abbreviation ORDER BY all_orders_per_customer.created_at) AS nth_order_after_init_flavour,
                max(
                CASE
                    WHEN date_trunc('month'::text,
                                    first_orders_per_flavour_enhanced.min_order_date_per_customer_per_flavour) =
                         date_trunc('month'::text, CURRENT_DATE::timestamp with time zone) THEN 1
                    ELSE NULL::integer
                    END)
                OVER (PARTITION BY all_orders_per_customer.customer_id, first_orders_per_flavour_enhanced.pod_flavour_first, first_orders_per_flavour_enhanced.country_abbreviation)                                                       AS customers_this_month,
                max(
                CASE
                    WHEN date_trunc('quarter'::text,
                                    first_orders_per_flavour_enhanced.min_order_date_per_customer_per_flavour) =
                         date_trunc('quarter'::text, CURRENT_DATE::timestamp with time zone) THEN 1
                    ELSE NULL::integer
                    END)
                OVER (PARTITION BY all_orders_per_customer.customer_id, first_orders_per_flavour_enhanced.pod_flavour_first, first_orders_per_flavour_enhanced.country_abbreviation)                                                       AS customers_this_quarter,
                max(
                CASE
                    WHEN date_trunc('year'::text,
                                    first_orders_per_flavour_enhanced.min_order_date_per_customer_per_flavour) =
                         date_trunc('year'::text, CURRENT_DATE::timestamp with time zone) THEN 1
                    ELSE NULL::integer
                    END)
                OVER (PARTITION BY all_orders_per_customer.customer_id, first_orders_per_flavour_enhanced.pod_flavour_first, first_orders_per_flavour_enhanced.country_abbreviation)                                                       AS customers_this_year,
                max(
                CASE
                    WHEN date_trunc('day'::text,
                                    first_orders_per_flavour_enhanced.min_order_date_per_customer_per_flavour) <=
                         date_trunc('day'::text, CURRENT_DATE::timestamp with time zone) THEN 1
                    ELSE NULL::integer
                    END)
                OVER (PARTITION BY all_orders_per_customer.customer_id, first_orders_per_flavour_enhanced.pod_flavour_first, first_orders_per_flavour_enhanced.country_abbreviation)                                                       AS customers_this_all,
                max(
                CASE
                    WHEN date_trunc('day'::text,
                                    first_orders_per_flavour_enhanced.min_order_date_per_customer_per_flavour) >=
                         (date_trunc('day'::text, CURRENT_DATE::timestamp with time zone) - '30 days'::interval) THEN 1
                    ELSE NULL::integer
                    END)
                OVER (PARTITION BY all_orders_per_customer.customer_id, first_orders_per_flavour_enhanced.pod_flavour_first, first_orders_per_flavour_enhanced.country_abbreviation)                                                       AS customers_last30days,
                max(
                CASE
                    WHEN date_trunc('day'::text,
                                    first_orders_per_flavour_enhanced.min_order_date_per_customer_per_flavour) >=
                         (date_trunc('day'::text, CURRENT_DATE::timestamp with time zone) - '90 days'::interval) THEN 1
                    ELSE NULL::integer
                    END)
                OVER (PARTITION BY all_orders_per_customer.customer_id, first_orders_per_flavour_enhanced.pod_flavour_first, first_orders_per_flavour_enhanced.country_abbreviation)                                                       AS customers_last90days
         FROM all_orders_per_customer
                  LEFT JOIN first_orders_per_flavour_enhanced
                            ON all_orders_per_customer.customer_id = first_orders_per_flavour_enhanced.customer_id
         WHERE all_orders_per_customer.created_at >=
               first_orders_per_flavour_enhanced.min_order_date_per_customer_per_flavour
     ),
     flavour_returning_customer_classification AS (
         SELECT all_orders_per_customer_enhanced.customer_id,
                all_orders_per_customer_enhanced.country_abbreviation,
                all_orders_per_customer_enhanced.order_id,
                all_orders_per_customer_enhanced.created_at,
                all_orders_per_customer_enhanced.pod_flavour_first,
                all_orders_per_customer_enhanced.min_order_date_per_customer_per_flavour,
                all_orders_per_customer_enhanced.nth_order_after_init_flavour,
                all_orders_per_customer_enhanced.starter_set_flag,
                all_orders_per_customer_enhanced.customers_this_month,
                all_orders_per_customer_enhanced.customers_this_quarter,
                all_orders_per_customer_enhanced.customers_this_year,
                all_orders_per_customer_enhanced.customers_this_all,
                all_orders_per_customer_enhanced.customers_last30days,
                all_orders_per_customer_enhanced.customers_last90days,
                max(
                CASE
                    WHEN all_orders_per_customer_enhanced.nth_order_after_init_flavour >= 2 AND
                         (all_orders_per_customer_enhanced.created_at -
                          all_orders_per_customer_enhanced.min_order_date_per_customer_per_flavour) <= '7 days'::interval
                        THEN 1
                    ELSE NULL::integer
                    END)
                OVER (PARTITION BY all_orders_per_customer_enhanced.customer_id, all_orders_per_customer_enhanced.pod_flavour_first, all_orders_per_customer_enhanced.country_abbreviation) AS returning_customer_7_days,
                max(
                CASE
                    WHEN all_orders_per_customer_enhanced.nth_order_after_init_flavour >= 2 AND
                         (all_orders_per_customer_enhanced.created_at -
                          all_orders_per_customer_enhanced.min_order_date_per_customer_per_flavour) <= '30 days'::interval
                        THEN 1
                    ELSE NULL::integer
                    END)
                OVER (PARTITION BY all_orders_per_customer_enhanced.customer_id, all_orders_per_customer_enhanced.pod_flavour_first, all_orders_per_customer_enhanced.country_abbreviation) AS returning_customer_30_days,
                max(
                CASE
                    WHEN all_orders_per_customer_enhanced.nth_order_after_init_flavour >= 2 AND
                         (all_orders_per_customer_enhanced.created_at -
                          all_orders_per_customer_enhanced.min_order_date_per_customer_per_flavour) <= '90 days'::interval
                        THEN 1
                    ELSE NULL::integer
                    END)
                OVER (PARTITION BY all_orders_per_customer_enhanced.customer_id, all_orders_per_customer_enhanced.pod_flavour_first, all_orders_per_customer_enhanced.country_abbreviation) AS returning_customer_90_days,
                max(
                CASE
                    WHEN all_orders_per_customer_enhanced.nth_order_after_init_flavour >= 2 THEN 1
                    ELSE NULL::integer
                    END)
                OVER (PARTITION BY all_orders_per_customer_enhanced.customer_id, all_orders_per_customer_enhanced.pod_flavour_first, all_orders_per_customer_enhanced.country_abbreviation) AS returning_customer_overall
         FROM all_orders_per_customer_enhanced
     )

SELECT
  *
FROM
  flavour_returning_customer_classification