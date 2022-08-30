----legacy: amazon.mws_cohort_processing_separate_initial_purchase
---Authors: Tomas Kristof

---###################################################################################################################

        ---compute amazon_returns---
 

---###################################################################################################################

 

 WITH order_agg AS (
         SELECT 
           ofse.amazon_order_id,
            ofse.country_fullname,
            ofse.sku,
            sum(ofse.quantity_shipped) AS total_sales
           FROM "airup_eu_dwh"."amazon"."fct_orders_fulfilled_shipments_enriched" ofse 
          GROUP BY ofse.amazon_order_id, ofse.country_fullname, ofse.sku
        ), 
        total_returns AS (
         SELECT date_trunc('day'::text, fcr.return_date)::date AS mont,
            oa.country_fullname AS country,
            cpfm.product_type,
            cpfm.pod_flavour,
            sum(fcr.quantity) AS total_returns,
            sum(
                CASE
                    WHEN fcr.detailed_disposition::text = 'SELLABLE'::text THEN 1
                    ELSE 0
                END) AS total_sellable,
            sum(
                CASE
                    WHEN fcr.detailed_disposition::text = 'DAMAGED'::text OR fcr.detailed_disposition::text = 'CARRIER_DAMAGED'::text THEN 1
                    ELSE 0
                END) AS total_destroyed_compensation,
            sum(
                CASE
                    WHEN fcr.detailed_disposition::text = 'DEFECTIVE'::text OR fcr.detailed_disposition::text = 'CUSTOMER_DAMAGED'::text THEN 1
                    ELSE 0
                END) AS total_destroyed_no_compensation
           FROM "airup_eu_dwh"."amazon"."fulfillment_customer_returns" fcr
             JOIN order_agg oa ON fcr.order_id::text = oa.amazon_order_id::text AND fcr.sku::text = oa.sku::text
             LEFT JOIN "airup_eu_dwh"."amazon"."custom_pod_flavour_mapping" cpfm ON fcr.product_name::text = cpfm.product_name::text
          WHERE fcr.return_date::date >= '2020-01-01'::date
          GROUP BY (date_trunc('day'::text, fcr.return_date)::date), oa.country_fullname, cpfm.product_type, cpfm.pod_flavour
          ORDER BY (date_trunc('day'::text, fcr.return_date)::date)
        ), 
        total_sales AS (
         SELECT date_trunc('day'::text, ofse.shipment_date)::date AS mont,
            ofse.country_fullname AS country,
            ofse.product_type,
            ofse.pod_flavour,
            sum(ofse.quantity_shipped) AS total_sales
           FROM "airup_eu_dwh"."amazon"."fct_orders_fulfilled_shipments_enriched" ofse 
          WHERE ofse.shipment_date::date >= '2020-01-01'::date
          GROUP BY (date_trunc('day'::text, ofse.shipment_date)::date), ofse.country_fullname, ofse.product_type, ofse.pod_flavour
        ), 
        final_join AS (
         SELECT ts.mont AS month,
            ts.country,
            ts.pod_flavour,
            ts.product_type,
            ts.total_sales,
            COALESCE(tr.total_returns, 0::numeric) AS total_returns,
            COALESCE(tr.total_sellable, 0::bigint) AS total_sellable,
            COALESCE(tr.total_destroyed_compensation, 0::bigint) AS total_destroyed_compensation,
            COALESCE(tr.total_destroyed_no_compensation, 0::bigint) AS total_destroyed_no_compensation
           FROM total_sales ts
             LEFT JOIN total_returns tr ON ts.country::text = tr.country::text AND ts.mont = tr.mont AND ts.product_type = tr.product_type AND ts.pod_flavour = tr.pod_flavour
        )
 SELECT final_join.month,
    final_join.country,
    final_join.pod_flavour,
    final_join.product_type,
    final_join.total_sales,
    final_join.total_returns,
    final_join.total_sellable,
    final_join.total_destroyed_compensation,
    final_join.total_destroyed_no_compensation
   FROM final_join