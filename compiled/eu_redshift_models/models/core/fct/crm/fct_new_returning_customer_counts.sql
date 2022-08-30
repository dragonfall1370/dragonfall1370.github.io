---legacy: crm.new_returning_customer_counts
  
SELECT 
        customer_id,
        date(foe.created_at) AS order_date,
        CASE
            WHEN (created_at - min(created_at) OVER (PARTITION BY customer_id, country_fullname)) = '00:00:00'::interval 
            THEN 'New Customer'::text
            ELSE 'Returning Customer'::text
        END AS customer_type,
          country_fullname AS country
  FROM "airup_eu_dwh"."shopify_global"."fct_order_enriched" foe
  WHERE financial_status in ('paid', 'partially_refunded') 
  AND customer_id IS NOT NULL