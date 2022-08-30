----legacy: reports.census_klaviyo_xscore
---Authors: Etoma Egot
---Last Modified by: Etoma Egot

---###################################################################################################################

        ---This query pushes computed xscores to klaviyo via census (reverse etl tool)---

---###################################################################################################################


SELECT 
   customer_id,
   email,
   xscore,
   customer_type,
   webshop,
   shop_country,
   last_pod_purchase_date,
   list_name
FROM(
WITH customer_purchases AS (
         select 
            distinct
                    foen.customer_id,
                    lower(foen.email) as email,
                    foen.shopify_shop AS webshop,
                    foen.shop_country as shop_country,
            max(date(foen.created_at)) AS last_pod_purchase_date,
            sum(fol.quantity) AS pods_purchased,
            count(DISTINCT foen.order_number) AS orders

           from  
              "airup_eu_dwh"."shopify_global"."fct_order_enriched_ngdpr" foen
             left join "airup_eu_dwh"."shopify_global"."fct_order_line" fol ON foen.id = fol.order_id
             left join "airup_eu_dwh"."shopify_global"."shopify_product_categorisation" product_categorisation
           on  fol.sku = product_categorisation.sku
             left join "airup_eu_dwh"."shopify_global"."seed_shopify_product_categorisation_ss_and_bundle_content" product_categorisation_ss_and_bundles
             on product_categorisation.sku = product_categorisation_ss_and_bundles.sku
          where lower(product_categorisation.category) in ('flavour', 'hardware')
          AND foen.financial_status in ('paid','partially_refunded')
          
          GROUP BY 
          foen.customer_id,
          foen.email, 
          foen.created_at, 
          foen.shopify_shop,
          foen.shop_country
          ORDER BY 
          foen.customer_id
        ), 
        pod_orders AS (
         select
            distinct   
                            customer_id,
                            email,
                            webshop,
                            shop_country,
            max(last_pod_purchase_date) AS last_pod_purchase_date,
            sum(orders) AS orders
           FROM 
                customer_purchases
          GROUP BY 
                customer_id, 
                email, 
                webshop,
                shop_country
          ORDER BY 
                
                customer_id
        ), 
        aggregation AS (
         SELECT 
                            pod_orders.customer_id,
                            pod_orders.email,
                            pod_orders.webshop,
                            pod_orders.shop_country,
            max(pod_orders.last_pod_purchase_date) AS last_pod_purchase_date,
            pod_orders.orders AS pod_orders,
            sum(pods_purchased) * 21::double precision AS pods_purchased_by_twentyonedays,
            CURRENT_DATE - pod_orders.last_pod_purchase_date AS days_since_last_purchase
           FROM pod_orders
             LEFT JOIN customer_purchases USING (customer_id, last_pod_purchase_date, email, webshop)
          GROUP BY pod_orders.customer_id, pod_orders.email, pod_orders.last_pod_purchase_date, pods_purchased, pod_orders.orders, pod_orders.webshop, pod_orders.shop_country
        )
    SELECT  *
    FROM 
         (SELECT 
               
                           aggregation.customer_id,
                           aggregation.email,
                           aggregation.webshop,
                           aggregation.shop_country,
                           aggregation.last_pod_purchase_date,
                           pods_purchased_by_twentyonedays - aggregation.days_since_last_purchase::double precision AS xscore,
               CASE
                    WHEN pod_orders = 1 AND (pods_purchased_by_twentyonedays - aggregation.days_since_last_purchase) > 0 
                         THEN 'new'::text
                    WHEN pod_orders = 1 AND (pods_purchased_by_twentyonedays - aggregation.days_since_last_purchase) <= 0 
                         THEN 'not_activated'::text
                    WHEN pod_orders > 1 AND (pods_purchased_by_twentyonedays - aggregation.days_since_last_purchase) >= 21 
                         THEN 'active'::text
                    WHEN pod_orders > 1 AND (pods_purchased_by_twentyonedays - aggregation.days_since_last_purchase) >= 0 AND (pods_purchased_by_twentyonedays - aggregation.days_since_last_purchase) < 21 
                         THEN 'at_risk'::text
                    WHEN pod_orders > 1 AND (pods_purchased_by_twentyonedays - aggregation.days_since_last_purchase) < -365
                         THEN 'churned'::text
                    WHEN pod_orders > 1 AND (pods_purchased_by_twentyonedays - aggregation.days_since_last_purchase) >= -365 AND (pods_purchased_by_twentyonedays - aggregation.days_since_last_purchase) <= 0 
                         THEN 'dormant'::text
                    ELSE 'others'::text
                END AS customer_type,
                           'Census'::text AS list_name,
                           row_number() OVER (PARTITION BY aggregation.customer_id ORDER BY aggregation.email, aggregation.webshop,aggregation.shop_country, aggregation.last_pod_purchase_date) AS id_ranked
            FROM 
               aggregation 
            WHERE aggregation.customer_id IS NOT NULL
            ORDER BY aggregation.email, aggregation.webshop,aggregation.shop_country, aggregation.last_pod_purchase_date
               ) AS ranked
 
      WHERE ranked.id_ranked = 1
)b