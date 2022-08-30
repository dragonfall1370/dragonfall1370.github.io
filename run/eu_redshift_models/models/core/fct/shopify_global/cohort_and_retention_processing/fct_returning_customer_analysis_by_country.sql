

  create view "airup_eu_dwh"."shopify_global"."fct_returning_customer_analysis_by_country__dbt_tmp" as (
    


WITH initial_orders AS (
	select *
	from
		(select
			foe.customer_id,
			foe.created_at  AS init_order_timestamp,
			foe.order_number AS init_order_number,
			rank() OVER (PARTITION BY foe.customer_id ORDER BY foe.created_at, foe.order_number) AS parent_id_ranked
		FROM "airup_eu_dwh"."shopify_global"."fct_order_enriched" foe
    	WHERE foe.financial_status in ('paid', 'partially_refunded')
		) AS ranked
    WHERE ranked.parent_id_ranked = 1),

     distinct_orders AS (
         SELECT DISTINCT foe.order_number,
                         foe.created_at,
                         COALESCE(country_system_account_mapping.country_abbreviation,
                                  'other') AS country_abbreviation,
                         sum(foe.net_revenue_2)               AS net_revenue
         FROM "airup_eu_dwh"."shopify_global"."fct_order_enriched" foe
                  LEFT JOIN "airup_eu_dwh"."public"."country_system_account_mapping" country_system_account_mapping
                    ON foe.shipping_address_country = country_system_account_mapping.shopify_shipping_address_country
                  LEFT JOIN "airup_eu_dwh"."shopify_global"."fct_order_shipping_line" fosl
                    ON foe.id = fosl.order_id
         WHERE foe.financial_status in ('paid', 'partially_refunded')
         GROUP BY foe.order_number, foe.created_at, country_system_account_mapping.country_abbreviation
     ),

--########################
--#### rollup replacement
--########################

    main_groupby as (
            SELECT CASE
                       WHEN initial_orders.init_order_number IS NULL THEN 'Returning D2C Customer'
                       ELSE 'New D2C Customer'
                       END                                                     AS returning_customer_flag,
                   count(DISTINCT initial_orders.customer_id)                  AS new_customers,
                   date(distinct_orders.created_at)                            AS order_date,
                   date(date_trunc('month', distinct_orders.created_at)) AS month_classification,
                   count(DISTINCT distinct_orders.order_number)                AS orders,
                   sum(distinct_orders.net_revenue)                            AS net_revenue,
                   distinct_orders.country_abbreviation                 AS country
            FROM distinct_orders
                     LEFT JOIN initial_orders ON distinct_orders.order_number = initial_orders.init_order_number
            GROUP BY (
                         CASE
                             WHEN initial_orders.init_order_number IS NULL THEN 'Returning D2C Customer'
                             ELSE 'New D2C Customer'
                             END), (date_trunc('month', distinct_orders.created_at)),
                     (date_trunc('day', distinct_orders.created_at)), (date(distinct_orders.created_at)),
                     distinct_orders.country_abbreviation
             ),

    total_agg as (
            SELECT CASE
                       WHEN initial_orders.init_order_number IS NULL THEN 'Returning D2C Customer'
                       ELSE 'New D2C Customer'
                       END                                                     AS returning_customer_flag,
                   count(DISTINCT initial_orders.customer_id)                  AS new_customers,
                   date(distinct_orders.created_at)                            AS order_date,
                   date(date_trunc('month', distinct_orders.created_at)) AS month_classification,
                   count(DISTINCT distinct_orders.order_number)                AS orders,
                   sum(distinct_orders.net_revenue)                            AS net_revenue,
                   'All'              AS country
            FROM distinct_orders
                     LEFT JOIN initial_orders ON distinct_orders.order_number = initial_orders.init_order_number
            GROUP BY (
                         CASE
                             WHEN initial_orders.init_order_number IS NULL THEN 'Returning D2C Customer'
                             ELSE 'New D2C Customer'
                             END), (date_trunc('month', distinct_orders.created_at)),
                     (date_trunc('day', distinct_orders.created_at)), (date(distinct_orders.created_at))
             ),

    union_cte as (
 	select * from main_groupby
    union all
    select * from total_agg)

select
    *
from
    union_cte
  ) with no schema binding;
