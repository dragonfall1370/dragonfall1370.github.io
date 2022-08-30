

WITH
all_orders_per_customer AS (
    SELECT order_enriched.customer_id,
           order_line.order_id,
           order_line.title,
           order_enriched.created_at,
           order_enriched.country_abbreviation,
		   case
		       when seed_flavours_ss.subcategory_4 is null then seed_flavours.subcategory_3
		       when seed_flavours_ss.subcategory_4 is not null then seed_flavours_ss.subcategory_4
		       else 'Unmapped Flavour'
		   end as pod_flavour,
           case when seed_flavours.subcategory_1 = 'Starter Set' then 'starter-set' else 'pod' end as starter_set_flag,
           min(order_enriched.created_at) OVER (PARTITION BY order_enriched.customer_id ORDER BY order_enriched.created_at rows between 1 following and 1 following) AS min_order_date_per_customer,
		   max(CASE WHEN order_enriched.campaign_purchase IS TRUE THEN 1 ELSE 0 END) OVER (PARTITION BY order_enriched.customer_id)                           AS campaign_customer
    --FROM dbt_feldm.fct_order_enriched_campaign_enriched_offline order_enriched
    from "airup_eu_dwh"."reports"."fct_order_enriched_campaign_enriched_offline" order_enriched
             LEFT JOIN shopify_global.fct_order_line order_line
                ON order_enriched.id = order_line.order_id
        	 LEFT JOIN shopify_global.seed_shopify_product_categorisation seed_flavours
        	    ON order_line.sku = seed_flavours.sku
        	 LEFT JOIN shopify_global.seed_shopify_product_categorisation_ss_and_bundle_content seed_flavours_ss
        		ON seed_flavours.sku = seed_flavours_ss.sku
    WHERE
    	seed_flavours.category in ('Flavour', 'Hardware')

	 ),

     first_orders_per_flavour AS (
         SELECT all_orders_per_customer.customer_id,
                all_orders_per_customer.pod_flavour     AS pod_flavour_first,
                min(all_orders_per_customer.created_at) AS min_order_date_per_customer_per_flavour
         FROM all_orders_per_customer
         GROUP BY
                all_orders_per_customer.customer_id,
                all_orders_per_customer.pod_flavour
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
                  first_orders_per_flavour.pod_flavour_first, first_orders_per_flavour.min_order_date_per_customer_per_flavour

     ),

     all_orders_per_customer_enhanced AS (
         SELECT all_orders_per_customer.customer_id,
                all_orders_per_customer.order_id,
                all_orders_per_customer.created_at,
                all_orders_per_customer.campaign_customer,
                all_orders_per_customer.min_order_date_per_customer,
                first_orders_per_flavour_enhanced.pod_flavour_first,
                first_orders_per_flavour_enhanced.min_order_date_per_customer_per_flavour,
                first_orders_per_flavour_enhanced.country_abbreviation,
                first_orders_per_flavour_enhanced.starter_set_flag,
                dense_rank()
                OVER (PARTITION BY first_orders_per_flavour_enhanced.customer_id, first_orders_per_flavour_enhanced.pod_flavour_first, first_orders_per_flavour_enhanced.country_abbreviation ORDER BY all_orders_per_customer.created_at) AS nth_order_after_init_flavour
         FROM all_orders_per_customer
                  LEFT JOIN first_orders_per_flavour_enhanced
                            ON all_orders_per_customer.customer_id = first_orders_per_flavour_enhanced.customer_id
         WHERE
               all_orders_per_customer.created_at >= first_orders_per_flavour_enhanced.min_order_date_per_customer_per_flavour
         	   and 	all_orders_per_customer.min_order_date_per_customer > '2021-10-01' -- reducing the amount of data
     ),


     flavour_returning_customer_classification AS (
         SELECT all_orders_per_customer_enhanced.customer_id,
                all_orders_per_customer_enhanced.country_abbreviation,
                all_orders_per_customer_enhanced.order_id,
                all_orders_per_customer_enhanced.campaign_customer,
                date(date_trunc('day', all_orders_per_customer_enhanced.min_order_date_per_customer)) as date,
                all_orders_per_customer_enhanced.pod_flavour_first,
                all_orders_per_customer_enhanced.min_order_date_per_customer_per_flavour,
                all_orders_per_customer_enhanced.nth_order_after_init_flavour,
                all_orders_per_customer_enhanced.starter_set_flag,
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
     ),



     final AS (
     with main_groupby as (
         SELECT flavour_returning_customer_classification.pod_flavour_first,
                flavour_returning_customer_classification.country_abbreviation,
                flavour_returning_customer_classification.campaign_customer,
                flavour_returning_customer_classification.starter_set_flag AS product_type,
                count(DISTINCT flavour_returning_customer_classification.customer_id) AS customers,

                flavour_returning_customer_classification.date, -- added a date here for datarange filtering

                count(DISTINCT
                      CASE
                          WHEN flavour_returning_customer_classification.returning_customer_7_days = 1
                              THEN flavour_returning_customer_classification.customer_id
                          ELSE NULL::text
                          END)                                                        AS returning_customers_7_days,
                count(DISTINCT
                      CASE
                          WHEN flavour_returning_customer_classification.returning_customer_30_days = 1
                              THEN flavour_returning_customer_classification.customer_id
                          ELSE NULL::text
                          END)                                                        AS returning_customers_30_days,
                count(DISTINCT
                      CASE
                          WHEN flavour_returning_customer_classification.returning_customer_90_days = 1
                              THEN flavour_returning_customer_classification.customer_id
                          ELSE NULL::text
                          END)                                                        AS returning_customers_90_days,
                count(DISTINCT
                      CASE
                          WHEN flavour_returning_customer_classification.returning_customer_overall = 1
                              THEN flavour_returning_customer_classification.customer_id
                          ELSE NULL::text
                          END)                                                        AS returning_customer_overall
         FROM flavour_returning_customer_classification
         GROUP BY flavour_returning_customer_classification.pod_flavour_first,
           		  flavour_returning_customer_classification.date,
           		  flavour_returning_customer_classification.campaign_customer,
           		  flavour_returning_customer_classification.country_abbreviation,
           		  flavour_returning_customer_classification.starter_set_flag
           		  ),
         total_groupby as (
         SELECT flavour_returning_customer_classification.pod_flavour_first,
                flavour_returning_customer_classification.country_abbreviation,
                flavour_returning_customer_classification.campaign_customer,
                'All'::text AS product_type,
                count(DISTINCT flavour_returning_customer_classification.customer_id) AS customers,

                flavour_returning_customer_classification.date, -- added a date here for datarange filtering

                count(DISTINCT
                      CASE
                          WHEN flavour_returning_customer_classification.returning_customer_7_days = 1
                              THEN flavour_returning_customer_classification.customer_id
                          ELSE NULL::text
                          END)                                                        AS returning_customers_7_days,
                count(DISTINCT
                      CASE
                          WHEN flavour_returning_customer_classification.returning_customer_30_days = 1
                              THEN flavour_returning_customer_classification.customer_id
                          ELSE NULL::text
                          END)                                                        AS returning_customers_30_days,
                count(DISTINCT
                      CASE
                          WHEN flavour_returning_customer_classification.returning_customer_90_days = 1
                              THEN flavour_returning_customer_classification.customer_id
                          ELSE NULL::text
                          END)                                                        AS returning_customers_90_days,
                count(DISTINCT
                      CASE
                          WHEN flavour_returning_customer_classification.returning_customer_overall = 1
                              THEN flavour_returning_customer_classification.customer_id
                          ELSE NULL::text
                          END)                                                        AS returning_customer_overall
         FROM flavour_returning_customer_classification
         GROUP BY flavour_returning_customer_classification.pod_flavour_first,
           		  flavour_returning_customer_classification.date,
           		  flavour_returning_customer_classification.campaign_customer,
           		  flavour_returning_customer_classification.country_abbreviation),
         union_groupby as (
         
         select m.pod_flavour_first, 
         m.country_abbreviation,
         m.campaign_customer,
         m.product_type,
         m.customers,
         m.date,
         m.returning_customers_7_days,
         m.returning_customers_30_days,
         m.returning_customers_90_days,
         m.returning_customer_overall
         from main_groupby m 
    		union
    	 select r.pod_flavour_first,
    	 r.country_abbreviation,
    	 r.campaign_customer,
    	 r.product_type,
    	 r.customers,
    	 r.date,
         r.returning_customers_7_days,
         r.returning_customers_30_days,
         r.returning_customers_90_days,
         r.returning_customer_overall
    	 from total_groupby r
    	 
         )
		 select * from union_groupby
         )
select
	*
from
	final