

WITH all_orders AS (
         SELECT order_enriched.customer_id,
            order_enriched.country_fullname AS country,
            order_enriched.created_at AS order_date,
            order_enriched.id AS order_id,
            order_enriched.total_price AS order_value
           FROM "airup_eu_dwh"."shopify_global"."fct_order_enriched" order_enriched
          WHERE order_enriched.financial_status::text IN ('paid', 'partially_refunded')
          ORDER BY order_enriched.customer_id
        ), initial_orders AS (
        SELECT * FROM (
            SELECT
            order_enriched.customer_id, 
            order_enriched.country_fullname AS country,
            order_enriched.created_at AS order_date,
            date_trunc('day', order_enriched.created_at)::date AS cohort_day,
            row_number() OVER (PARTITION BY order_enriched.customer_id, order_enriched.country_fullname ORDER BY order_enriched.created_at) id_ranked
           FROM "airup_eu_dwh"."shopify_global"."fct_order_enriched" order_enriched
          WHERE order_enriched.financial_status IN ('paid', 'partially_refunded')
          ORDER BY order_enriched.customer_id, order_enriched.country_fullname, order_enriched.created_at ) ranked
          WHERE ranked.id_ranked = 1
        ), new_returning_customers AS (
         SELECT all_orders.customer_id,
            all_orders.country,
            all_orders.order_id,
            all_orders.order_value,
            all_orders.order_date::date AS order_date,
                CASE
                    WHEN (all_orders.order_date - initial_orders.order_date) > '00:00:00'::interval THEN 1
                    ELSE 0
                END AS returning_customer,
                CASE
                    WHEN (all_orders.order_date - initial_orders.order_date) = '00:00:00'::interval THEN 1
                    ELSE 0
                END AS new_customer,
            count(DISTINCT all_orders.order_id) AS total_orders
           FROM all_orders
             LEFT JOIN initial_orders USING (customer_id, country)
          GROUP BY all_orders.customer_id, all_orders.country, all_orders.order_id, all_orders.order_value, all_orders.order_date, initial_orders.order_date
          ORDER BY all_orders.customer_id
        ), agg_customer_order_value AS (
         SELECT new_returning_customers.country,
            new_returning_customers.order_date,
            'Shopify'::text AS data_source,
            sum(new_returning_customers.order_value) AS total_order_value,
            sum(new_returning_customers.returning_customer) AS returning_customer_count,
            sum(new_returning_customers.new_customer) AS new_customer_count,
            sum(new_returning_customers.total_orders) AS total_orders
           FROM new_returning_customers
          GROUP BY new_returning_customers.country, new_returning_customers.order_date
          ORDER BY new_returning_customers.country, new_returning_customers.order_date
        ), amazon_all_orders AS (
         SELECT orders_fulfilled_shipments_manual_upload_enriched.buyer_email,
            orders_fulfilled_shipments_manual_upload_enriched.purchase_date,
            orders_fulfilled_shipments_manual_upload_enriched.country_fullname AS country
          FROM "airup_eu_dwh"."amazon"."fct_orders_fulfilled_shipments_enriched" orders_fulfilled_shipments_manual_upload_enriched
          GROUP BY orders_fulfilled_shipments_manual_upload_enriched.buyer_email, orders_fulfilled_shipments_manual_upload_enriched.purchase_date, orders_fulfilled_shipments_manual_upload_enriched.country_fullname
          ORDER BY orders_fulfilled_shipments_manual_upload_enriched.buyer_email
        ), amazon_initial_orders AS (
        SELECT * FROM (
            SELECT
            orders_fulfilled_shipments_manual_upload_enriched.buyer_email, 
            orders_fulfilled_shipments_manual_upload_enriched.country_fullname AS country,
            orders_fulfilled_shipments_manual_upload_enriched.purchase_date,
            row_number() OVER (PARTITION BY orders_fulfilled_shipments_manual_upload_enriched.buyer_email, orders_fulfilled_shipments_manual_upload_enriched.country_fullname ORDER BY orders_fulfilled_shipments_manual_upload_enriched.purchase_date) id_ranked
           FROM "airup_eu_dwh"."amazon"."fct_orders_fulfilled_shipments_enriched" orders_fulfilled_shipments_manual_upload_enriched
          ORDER BY orders_fulfilled_shipments_manual_upload_enriched.buyer_email, orders_fulfilled_shipments_manual_upload_enriched.country_fullname, orders_fulfilled_shipments_manual_upload_enriched.purchase_date ) ranked
          WHERE ranked.id_ranked = 1
        ), amazon_new_returning_customers AS (
         SELECT amazon_all_orders.buyer_email,
            amazon_all_orders.country,
            amazon_all_orders.purchase_date::date AS purchase_date,
                CASE
                    WHEN (amazon_all_orders.purchase_date::date - amazon_initial_orders.purchase_date::date) > '00:00:00'::interval THEN 1
                    ELSE 0
                END AS amazon_returning_customer,
                CASE
                    WHEN (amazon_all_orders.purchase_date::date - amazon_initial_orders.purchase_date::date) = '00:00:00'::interval THEN 1
                    ELSE 0
                END AS amazon_new_customer
           FROM amazon_all_orders
             LEFT JOIN amazon_initial_orders USING (buyer_email, country)
        ), agg_amazon_customers AS (
         SELECT amazon_new_returning_customers.country,
            amazon_new_returning_customers.purchase_date AS order_date,
            'Amazon'::text AS data_source,
            0::double precision AS total_order_value,
            sum(amazon_new_returning_customers.amazon_returning_customer) AS returning_customer_count,
            sum(amazon_new_returning_customers.amazon_new_customer) AS new_customer_count,
            0::double precision AS total_orders
           FROM amazon_new_returning_customers
          GROUP BY amazon_new_returning_customers.country, amazon_new_returning_customers.purchase_date
        ), agg_offline_retail_customers AS (
         SELECT 
            'Germany'::text AS country,
            "date" AS order_date,
            'Offline Retail'::text AS data_source,
            0 AS total_order_value,
            0 AS returning_customer_count,
            sum(starter_sets)::bigint AS new_customer_count,
            0 AS total_orders
           FROM "airup_eu_dwh"."odoo"."seed_offline_sales_excel"
           GROUP BY "date"
        )
 SELECT agg_customer_order_value.country,
    agg_customer_order_value.order_date,
    agg_customer_order_value.data_source,
    agg_customer_order_value.total_order_value,
    agg_customer_order_value.returning_customer_count,
    agg_customer_order_value.new_customer_count,
    agg_customer_order_value.total_orders
   FROM agg_customer_order_value
UNION ALL
 SELECT agg_amazon_customers.country,
    agg_amazon_customers.order_date,
    agg_amazon_customers.data_source,
    agg_amazon_customers.total_order_value,
    agg_amazon_customers.returning_customer_count,
    agg_amazon_customers.new_customer_count,
    agg_amazon_customers.total_orders
   FROM agg_amazon_customers
UNION ALL
 SELECT agg_offline_retail_customers.country,
    agg_offline_retail_customers.order_date,
    agg_offline_retail_customers.data_source,
    agg_offline_retail_customers.total_order_value,
    agg_offline_retail_customers.returning_customer_count,
    agg_offline_retail_customers.new_customer_count,
    agg_offline_retail_customers.total_orders
   FROM agg_offline_retail_customers