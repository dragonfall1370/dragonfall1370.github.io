----legacy:  amazon.amazon_sales
---Authors: Tomas Kristof
---Last Modified by: Oscar Higson Spence

---###################################################################################################################

        ---compute amazon_sales---

---###################################################################################################################

 

SELECT ofse._fivetran_synced::date AS last_updated,
       ofse.purchase_date::date    AS date,
       ofse.country_fullname       AS country,
       ofse.product_type,
       ofse.product_name,
       ofse.sku,
       sum(ofse.quantity_shipped)  AS items_sold,
       sum(fadpo.net_orders)         AS orders,
       sum(fadpo.gross_revenue)      AS sales,
       sum(fadpo.net_revenue_1)      AS net_sales,
       'Amazon'::text                AS channel,
       csm.asin,
       csm.airup_sku
FROM 
       "airup_eu_dwh"."amazon"."fct_orders_fulfilled_shipments_enriched" ofse
         LEFT JOIN "airup_eu_dwh"."amazon"."fct_aggregated_data_per_order" fadpo ON ofse.amazon_order_id::text = fadpo.amazon_order_id::text
         LEFT JOIN amazon.custom_sku_mapping csm ON ofse.sku::text = csm.sku::text
WHERE ofse.returned IS FALSE -- filtering out returned orders
GROUP BY (ofse._fivetran_synced::date), (ofse.purchase_date::date), ofse.country_fullname, ofse.product_type,
         ofse.product_name, ofse.sku, 'Amazon'::text, csm.asin, csm.airup_sku