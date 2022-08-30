---legacy: crm.retention_cycle_snapshots_country_weekly
---Author: Abhishek Pathak


---###################################################################################################################

                      ---Extract customer latest orders from shopify---

---###################################################################################################################
  
WITH customer_purchases AS (
         SELECT 
             DISTINCT 
                        foe.customer_id,
                        foe.country_fullname AS country,
               max(date(foe.created_at)) AS last_pod_purchase_date,
               sum(fol.quantity) AS pods_purchased,
               count(DISTINCT foe.order_number) AS orders

         FROM "airup_eu_dwh"."shopify_global"."fct_order_enriched" foe
             LEFT JOIN "airup_eu_dwh"."shopify_global"."fct_order_line" fol ON foe.id = fol.order_id
             LEFT JOIN "airup_eu_dwh"."shopify_global"."dim_product_enriched" dpe ON fol.product_id = dpe.id
             WHERE (dpe.product_type in ('Pod', 'Starter Kit', 'Aromapod', 'Aromapod-Bundle', 'Aromapod-Bundle-Mix'))
             AND foe.financial_status = 'paid' ----only fully paying customers are considered. 
             
          GROUP BY 
              foe.customer_id,
               foe.country_fullname,
                foe.created_at
          ORDER BY
              foe.customer_id,
               foe.country_fullname
        ), 
---###################################################################################################################

                      ---Extract distinct pod orders from customer purchases cte---

---###################################################################################################################
        pod_orders AS (
         SELECT 
              DISTINCT 
                      customer_id,
                      country,
              max(last_pod_purchase_date) AS last_pod_purchase_date,
              sum(orders) AS orders

         FROM customer_purchases

         GROUP BY
                     customer_id,
                     country
          ORDER BY
                     customer_id
        ),  
---###################################################################################################################

                      ---segmenting the customers by weekly purchase frequency---

---###################################################################################################################
        aggregation AS (
         SELECT 
                      pod_orders.customer_id,
                      pod_orders.country,
                      pod_orders.last_pod_purchase_date,
                      pod_orders.orders AS pod_orders,
                      customer_purchases.pods_purchased AS pods_purchased_on_last_purchase_date,
            sum(customer_purchases.pods_purchased) * 21 AS pods_purchased_by_twentyonedays,
            CURRENT_DATE - 1 - pod_orders.last_pod_purchase_date AS days_since_last_purchase
           FROM pod_orders
          LEFT JOIN customer_purchases USING (customer_id, country, last_pod_purchase_date)
          GROUP BY 
          pod_orders.customer_id,
           pod_orders.country,
            pod_orders.last_pod_purchase_date,
             customer_purchases.pods_purchased,
              pod_orders.orders
        ), 
---###################################################################################################################

                      ---classifying customers by segment and computing model x-score---

---###################################################################################################################
        by_current_date AS (
         select customer_id,
        country,
        last_pod_purchase_date,
        xscore,
        customer_type
        from
         (SELECT --DISTINCT ON (aggregation.customer_id, aggregation.country) # Removing the distinct on clause as it is not available on redshift. Using row_number() workaround instead.
         	aggregation.customer_id,
            aggregation.country,
            aggregation.last_pod_purchase_date,
            aggregation.pods_purchased_by_twentyonedays - aggregation.days_since_last_purchase AS xscore,
            row_number() over (partition by aggregation.customer_id, aggregation.country order by aggregation.customer_id, aggregation.country) as id_ranked,
                CASE
                    WHEN aggregation.pod_orders = 1 AND (aggregation.pods_purchased_by_twentyonedays - aggregation.days_since_last_purchase) > 0 
                          THEN 'new'::text  
                    WHEN aggregation.pod_orders = 1 AND (aggregation.pods_purchased_by_twentyonedays - aggregation.days_since_last_purchase) <= 0 
                          THEN 'not_activated'::text
                    WHEN aggregation.pod_orders > 1 AND (aggregation.pods_purchased_by_twentyonedays - aggregation.days_since_last_purchase) >= 21 
                          THEN 'active'::text
                    WHEN aggregation.pod_orders > 1 AND (aggregation.pods_purchased_by_twentyonedays - aggregation.days_since_last_purchase) >= 0 AND (aggregation.pods_purchased_by_twentyonedays - aggregation.days_since_last_purchase) < 21 
                          THEN 'at_risk'::text
                    WHEN aggregation.pod_orders > 1 AND (aggregation.pods_purchased_by_twentyonedays - aggregation.days_since_last_purchase) < '-365'::integer 
                          THEN 'churned'::text
                    WHEN aggregation.pod_orders > 1 AND (aggregation.pods_purchased_by_twentyonedays - aggregation.days_since_last_purchase) >= '-365'::integer AND (aggregation.pods_purchased_by_twentyonedays - aggregation.days_since_last_purchase) <= 0 
                          THEN 'dormant'::text
                    ELSE 'others'::text
                END AS customer_type
           FROM aggregation) as ranked
           where ranked.id_ranked = 1
        ), 
---###################################################################################################################

                      ---computing customer segmentation by snapshot week---

---###################################################################################################################

        by_current_date_agg AS (
         SELECT   
                  by_current_date.country,
                  CURRENT_DATE - 1 AS snapshot_week,
                  by_current_date.customer_type,
                count(*) AS share
         FROM by_current_date
         GROUP BY
             by_current_date.country,
              by_current_date.customer_type
        )
      SELECT
                by_current_date_agg.country,
                by_current_date_agg.customer_type,
                by_current_date_agg.snapshot_week,
                by_current_date_agg.share,
            sum(by_current_date_agg.share) OVER (PARTITION BY by_current_date_agg.snapshot_week) AS total_share_per_country
      FROM by_current_date_agg