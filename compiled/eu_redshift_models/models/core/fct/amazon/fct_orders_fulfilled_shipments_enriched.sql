----legacy: amazon.orders_fs_manual_upload_enriched
---Authors: Erik Klemusch,  Etoma Egot
---Last Modified by: Tomas Kristof

---###################################################################################################################

        ---compute amazon fulfilled shipments while accounting for returns---

---###################################################################################################################

 
with
    /* query bringing information about returned products; grouped due to duplicate rows caused by the license number */
    returns_prep as (
        select
            fcr.order_id,
            fcr.product_name,
            fcr.return_date
        from
            "airup_eu_dwh"."amazon"."fulfillment_customer_returns" fcr
  
        group by
            1,2,3
    )
---###################################################################################################################
/* original query bringing most of the information form the fs table
   TODO: Resolve the missing orders in the orders_fulfilled table (end of 2019, beg. of 2020)
   TODO: find a better join for the return table, e.g., ON amazon_order_item_id; those are currently not available */
---###################################################################################################################
SELECT
        fs.amazon_order_id,
        fs.amazon_order_item_id,
        fs.shipment_item_id,
        fs.shipment_date,
        fs.gift_wrap_tax,
        fs.ship_promotion_discount,
        fs.fulfillment_channel,
        fs.reporting_date,
        fs.sku,
        fs.ship_city,
        fs.ship_postal_code,
        fs.buyer_email,
        fs.shipping_price,
        fs._fivetran_batch,
        fs.sales_channel,
        fs.shipment_id,
        fs.product_name,
        fs._fivetran_index,
        fs.payments_date,
        fs.purchase_date,
        fs.ship_service_level,
        fs.quantity_shipped,
        fs.ship_state,
        fs.item_tax,
        fs.item_promotion_discount,
        fs.tracking_number,
        fs.currency,
        fs.fulfillment_center_id,
        fs.shipping_tax,
        fs.item_price,
        fs.estimated_arrival_date,
        fs.carrier,
        fs.gift_wrap_price,
        fs.ship_country,
        fs._fivetran_synced,
        COALESCE(csam.country_fullname, 'other')    AS country_fullname,
        COALESCE(csam.country_abbreviation,
                'other')                                                     AS country_abbreviation,
        COALESCE(csam.country_grouping, 'other')    AS country_grouping,
        cpfm.product_type,
        cpfm.pod_flavour,
  ---###################################################################################################################
    /* checking if the product exists in the return_prep table;
      TODO: implement a better join when amazon_order_item_id is available in the return table */
  ---###################################################################################################################
     CASE 
        WHEN returns_prep.product_name IS NOT NULL THEN TRUE ELSE FALSE END AS returned
FROM 
     "airup_eu_dwh"."amazon"."fulfilled_shipments" fs
    LEFT JOIN "airup_eu_dwh"."public"."country_system_account_mapping" csam ON fs.ship_country = csam.amazon_ship_country
    LEFT JOIN "airup_eu_dwh"."amazon"."custom_pod_flavour_mapping" cpfm ON fs.product_name = cpfm.product_name
    LEFT JOIN returns_prep
              ON returns_prep.order_id = fs.amazon_order_id
---####################################################################################################################################
    /* the following join condition was implemented due to inconsistencies across the product names in the tables, e.g., ü,®.
      these two regexp_replace statements are removing special characters and ü from both of the product names in both of the tables*/
---####################################################################################################################################
               AND regexp_replace(fs.product_name, 'ü', '', 1, 'i')
                        = regexp_replace(returns_prep.product_name, 'ü', '', 1, 'i')