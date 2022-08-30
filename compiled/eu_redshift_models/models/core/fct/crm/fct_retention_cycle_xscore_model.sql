---legacy: crm.retention_cycle_snapshots_country

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
             AND foe.financial_status in ('paid', 'partially_refunded') ----only paying customers are considered. 
             
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
            CURRENT_DATE - pod_orders.last_pod_purchase_date AS days_since_last_purchase,
            date(CURRENT_DATE - '7 days'::interval) - pod_orders.last_pod_purchase_date AS days_since_last_purchase_by_7days,
            date(CURRENT_DATE - '14 days'::interval) - pod_orders.last_pod_purchase_date AS days_since_last_purchase_by_14days,
            date(CURRENT_DATE - '21 days'::interval) - pod_orders.last_pod_purchase_date AS days_since_last_purchase_by_21days,
            date(CURRENT_DATE - '30 days'::interval) - pod_orders.last_pod_purchase_date AS days_since_last_purchase_by_30days

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
             xscore,
             customer_type
          from
         (SELECT 
          --    DISTINCT ON (customer_id, country) customer_id, # Removing the distinct on clause as it is not available on redshift. Using row_number() workaround instead.
            customer_id,
            country,
            pods_purchased_by_twentyonedays - days_since_last_purchase AS xscore,
                CASE
                    WHEN pod_orders = 1 AND (pods_purchased_by_twentyonedays - days_since_last_purchase) > 0 
                         THEN 'new'::text
                    WHEN pod_orders = 1 AND (pods_purchased_by_twentyonedays - days_since_last_purchase) <= 0 
                         THEN 'not_activated'::text
                    WHEN pod_orders > 1 AND (pods_purchased_by_twentyonedays - days_since_last_purchase) >= 21 
                         THEN 'active'::text
                    WHEN pod_orders > 1 AND (pods_purchased_by_twentyonedays - days_since_last_purchase) >= 0 AND (pods_purchased_by_twentyonedays - days_since_last_purchase) < 21 
                         THEN 'at_risk'::text
                    WHEN pod_orders > 1 AND (pods_purchased_by_twentyonedays - days_since_last_purchase) < '-365'::integer 
                         THEN 'churned'::text
                    WHEN pod_orders > 1 AND (pods_purchased_by_twentyonedays - days_since_last_purchase) >= '-365'::integer AND (pods_purchased_by_twentyonedays - days_since_last_purchase) <= 0 
                         THEN 'dormant'::text
                    ELSE 'others'::text
                END AS customer_type,
            row_number() over(PARTITION BY customer_id, country order by customer_id, country) as id_ranked
           FROM aggregation) as ranked
           where ranked.id_ranked = 1
        ), 
           by_last_7days AS (
          select customer_id,
             country,
             xscore,
             customer_type
          from
         (SELECT 
          -- DISTINCT ON (customer_id, country) customer_id, # Removing the distinct on clause as it is not available on redshift. Using row_number() workaround instead.
            customer_id,
            country,
            pods_purchased_by_twentyonedays - days_since_last_purchase AS xscore,
                CASE
                    WHEN pod_orders = 1 AND (pods_purchased_by_twentyonedays - days_since_last_purchase_by_7days) > 0 
                         THEN 'new'::text
                    WHEN pod_orders = 1 AND (pods_purchased_by_twentyonedays - days_since_last_purchase_by_7days) <= 0 
                         THEN 'not_activated'::text
                    WHEN pod_orders > 1 AND (pods_purchased_by_twentyonedays - days_since_last_purchase_by_7days) >= 21 
                         THEN 'active'::text
                    WHEN pod_orders > 1 AND (pods_purchased_by_twentyonedays - days_since_last_purchase_by_7days) >= 0 AND (pods_purchased_by_twentyonedays - days_since_last_purchase) < 21 
                         THEN 'at_risk'::text
                    WHEN pod_orders > 1 AND (pods_purchased_by_twentyonedays - days_since_last_purchase_by_7days) < '-365'::integer 
                         THEN 'churned'::text
                    WHEN pod_orders > 1 AND (pods_purchased_by_twentyonedays - days_since_last_purchase_by_7days) >= '-365'::integer AND (pods_purchased_by_twentyonedays - days_since_last_purchase) <= 0 
                         THEN 'dormant'::text
                    ELSE 'others'::text
                END AS customer_type,
             row_number() over(PARTITION BY customer_id, country order by customer_id, country) as id_ranked
           FROM aggregation) as ranked
           where ranked.id_ranked = 1
        ), 
          by_last_14days AS (
          select customer_id,
             country,
             xscore,
             customer_type
          from
         (SELECT 
          --  DISTINCT ON (customer_id, country) customer_id, # Removing the distinct on clause as it is not available on redshift. Using row_number() workaround instead.
            customer_id,
            country,
            pods_purchased_by_twentyonedays - days_since_last_purchase AS xscore,
                CASE
                    WHEN pod_orders = 1 AND (pods_purchased_by_twentyonedays - days_since_last_purchase_by_14days) > 0 
                        THEN 'new'::text
                    WHEN pod_orders = 1 AND (pods_purchased_by_twentyonedays - days_since_last_purchase_by_14days) <= 0 
                        THEN 'not_activated'::text
                    WHEN pod_orders > 1 AND (pods_purchased_by_twentyonedays - days_since_last_purchase_by_14days) >= 21 
                        THEN 'active'::text
                    WHEN pod_orders > 1 AND (pods_purchased_by_twentyonedays - days_since_last_purchase_by_14days) >= 0 AND (pods_purchased_by_twentyonedays - days_since_last_purchase) < 21 
                        THEN 'at_risk'::text
                    WHEN pod_orders > 1 AND (pods_purchased_by_twentyonedays - days_since_last_purchase_by_14days) < '-365'::integer 
                        THEN 'churned'::text
                    WHEN pod_orders > 1 AND (pods_purchased_by_twentyonedays - days_since_last_purchase_by_14days) >= '-365'::integer AND (pods_purchased_by_twentyonedays - days_since_last_purchase) <= 0 
                        THEN 'dormant'::text
                    ELSE 'others'::text
                END AS customer_type,
             row_number() over(PARTITION BY customer_id, country order by customer_id, country) as id_ranked
           FROM aggregation) as ranked
           where ranked.id_ranked = 1
        ), 
          by_last_21days AS (
          select customer_id,
             country,
             xscore,
             customer_type
          from
         (SELECT 
          --  DISTINCT ON (customer_id, country) customer_id, # Removing the distinct on clause as it is not available on redshift. Using row_number() workaround instead.
            customer_id,
            country,
            pods_purchased_by_twentyonedays - days_since_last_purchase AS xscore,
                CASE
                    WHEN pod_orders = 1 AND (pods_purchased_by_twentyonedays - days_since_last_purchase_by_21days) > 0 
                         THEN 'new'::text
                    WHEN pod_orders = 1 AND (pods_purchased_by_twentyonedays - days_since_last_purchase_by_21days) <= 0 
                         THEN 'not_activated'::text
                    WHEN pod_orders > 1 AND (pods_purchased_by_twentyonedays - days_since_last_purchase_by_21days) >= 21 
                         THEN 'active'::text
                    WHEN pod_orders > 1 AND (pods_purchased_by_twentyonedays - days_since_last_purchase_by_21days) >= 0 AND (pods_purchased_by_twentyonedays - days_since_last_purchase) < 21 
                         THEN 'at_risk'::text
                    WHEN pod_orders > 1 AND (pods_purchased_by_twentyonedays - days_since_last_purchase_by_21days) < '-365'::integer 
                         THEN 'churned'::text
                    WHEN pod_orders > 1 AND (pods_purchased_by_twentyonedays - days_since_last_purchase_by_21days) >= '-365'::integer AND (pods_purchased_by_twentyonedays - days_since_last_purchase) <= 0 
                         THEN 'dormant'::text
                    ELSE 'others'::text
                END AS customer_type,
            row_number() over(PARTITION BY customer_id, country order by customer_id, country) as id_ranked
           FROM aggregation) as ranked
           where ranked.id_ranked = 1
        ), 
           by_last_30days AS (
          select customer_id,
             country,
             xscore,
             customer_type
          from
         (SELECT 
          --   DISTINCT ON (customer_id, country) customer_id, # Removing the distinct on clause as it is not available on redshift. Using row_number() workaround instead.
            customer_id,
            country,
            pods_purchased_by_twentyonedays - days_since_last_purchase AS xscore,
                CASE
                    WHEN pod_orders = 1 AND (pods_purchased_by_twentyonedays - days_since_last_purchase_by_30days) > 0 
                         THEN 'new'::text
                    WHEN pod_orders = 1 AND (pods_purchased_by_twentyonedays - days_since_last_purchase_by_30days) <= 0 
                         THEN 'not_activated'::text
                    WHEN pod_orders > 1 AND (pods_purchased_by_twentyonedays - days_since_last_purchase_by_30days) >= 21 
                         THEN 'active'::text
                    WHEN pod_orders > 1 AND (pods_purchased_by_twentyonedays - days_since_last_purchase_by_30days) >= 0 AND (pods_purchased_by_twentyonedays - days_since_last_purchase) < 21 
                         THEN 'at_risk'::text
                    WHEN pod_orders > 1 AND (pods_purchased_by_twentyonedays - days_since_last_purchase_by_30days) < '-365'::integer 
                         THEN 'churned'::text
                    WHEN pod_orders > 1 AND (pods_purchased_by_twentyonedays - days_since_last_purchase_by_30days) >= '-365'::integer AND (pods_purchased_by_twentyonedays - days_since_last_purchase) <= 0 
                         THEN 'dormant'::text
                    ELSE 'others'::text
                END AS customer_type,
            row_number() over(PARTITION BY customer_id, country order by customer_id, country) as id_ranked
           FROM aggregation) as ranked
           where ranked.id_ranked = 1
        ), 
           snapshots_agg AS (
         SELECT 
                       'Today'::text AS week,
                        by_current_date.customer_type,
                        by_current_date.country,
                  count(*) AS share
         FROM by_current_date
         GROUP BY 
                        by_current_date.customer_type,
                        by_current_date.country

        UNION ALL

         SELECT 
                        'Last_7days'::text AS week,
                        by_last_7days.customer_type,
                        by_last_7days.country,
                  count(*) AS share
          FROM by_last_7days
          GROUP BY 
                        by_last_7days.customer_type,
                        by_last_7days.country

        UNION ALL

         SELECT 
                          'Last_14days'::text AS week,
                          by_last_14days.customer_type,
                          by_last_14days.country,
                    count(*) AS share
          FROM by_last_14days
          GROUP BY 
                          by_last_14days.customer_type,
                          by_last_14days.country

        UNION ALL

         SELECT 
                          'Last_21days'::text AS week,
                          by_last_21days.customer_type,
                          by_last_21days.country,
                    count(*) AS share
          FROM by_last_21days
          GROUP BY 
                          by_last_21days.customer_type,
                          by_last_21days.country

        UNION ALL

         SELECT 
                          'Last_30days'::text AS week,
                          by_last_30days.customer_type,
                          by_last_30days.country,
                    count(*) AS share
         FROM by_last_30days
         GROUP BY 
                          by_last_30days.customer_type,
                          by_last_30days.country

        ), 
        
        snapshot_share AS (
         SELECT
                CASE
                    WHEN snapshots_agg.week = 'Today'::text THEN 1
                    WHEN snapshots_agg.week = 'Last_7days'::text THEN 2
                    WHEN snapshots_agg.week = 'Last_14days'::text THEN 3
                    WHEN snapshots_agg.week = 'Last_21days'::text THEN 4
                    WHEN snapshots_agg.week = 'Last_30days'::text THEN 5
                    ELSE NULL::integer
                END AS row_id,
            snapshots_agg.week,
            snapshots_agg.customer_type,
            snapshots_agg.country,
            snapshots_agg.share,
            sum(snapshots_agg.share) OVER (PARTITION BY snapshots_agg.week, snapshots_agg.country) AS total_share
        FROM snapshots_agg

          WHERE snapshots_agg.customer_type <> 'others'::text
          GROUP BY (
                CASE
                    WHEN snapshots_agg.week = 'Today'::text THEN 1
                    WHEN snapshots_agg.week = 'Last_7days'::text THEN 2
                    WHEN snapshots_agg.week = 'Last_14days'::text THEN 3
                    WHEN snapshots_agg.week = 'Last_21days'::text THEN 4
                    WHEN snapshots_agg.week = 'Last_30days'::text THEN 5
                    ELSE NULL::integer
                END), snapshots_agg.week, snapshots_agg.customer_type, snapshots_agg.country, snapshots_agg.share
          ORDER BY (
                CASE
                    WHEN snapshots_agg.week = 'Today'::text THEN 1
                    WHEN snapshots_agg.week = 'Last_7days'::text THEN 2
                    WHEN snapshots_agg.week = 'Last_14days'::text THEN 3
                    WHEN snapshots_agg.week = 'Last_21days'::text THEN 4
                    WHEN snapshots_agg.week = 'Last_30days'::text THEN 5
                    ELSE NULL::integer
                END)
        )
      SELECT    
                snapshot_share.row_id,
                snapshot_share.week,
                snapshot_share.customer_type,
                snapshot_share.country,
                snapshot_share.share,
                snapshot_share.total_share,
                snapshot_share.share::double precision / snapshot_share.total_share::double precision AS pct_share
       FROM snapshot_share
       ORDER BY 
       snapshot_share.row_id,
        (snapshot_share.share::double precision / snapshot_share.total_share::double precision)