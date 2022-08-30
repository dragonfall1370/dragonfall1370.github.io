--- creator: Nham Dao
--- last modify: Nham Dao
--- summary: summary of number of orders we receive tracking information each hour


 WITH iqi_get_hour AS (
         SELECT date(fct_logistics_leadtime_data.iqi_create_date) AS iqi_create_date,
            extract(hour from fct_logistics_leadtime_data.iqi_create_date) AS hour_in_day,
            count(fct_logistics_leadtime_data.iqi_name_ref) AS number_of_order_iqi,
            COALESCE(fct_logistics_leadtime_data.shopify_country, 'Other'::character varying) AS shopify_country,
            COALESCE(so_warehouse_id, 'Other'::text) AS so_warehouse_id
           FROM "airup_eu_dwh"."odoo"."fct_logistics_leadtime_data"
          WHERE fct_logistics_leadtime_data.iqi_create_date IS NOT NULL
          GROUP BY date(fct_logistics_leadtime_data.iqi_create_date), extract(hour from fct_logistics_leadtime_data.iqi_create_date), fct_logistics_leadtime_data.shopify_country, so_warehouse_id
        ), ie_get_hour AS (
         SELECT date(fct_logistics_leadtime_data.ie_create_date) AS ie_create_date,
            extract(hour from fct_logistics_leadtime_data.ie_create_date) AS hour_in_day,
            count(fct_logistics_leadtime_data.ie_create_date) AS number_of_order_ie,
            COALESCE(fct_logistics_leadtime_data.shopify_country, 'Other'::character varying) AS shopify_country,
            COALESCE(so_warehouse_id, 'Other'::text) AS so_warehouse_id
           FROM "airup_eu_dwh"."odoo"."fct_logistics_leadtime_data"
          WHERE fct_logistics_leadtime_data.ie_create_date IS NOT NULL
          GROUP BY date(fct_logistics_leadtime_data.ie_create_date), extract(hour from fct_logistics_leadtime_data.ie_create_date), fct_logistics_leadtime_data.shopify_country, so_warehouse_id
        ), 
        country_list AS (
         SELECT DISTINCT foe.shipping_address_country as country
           FROM "airup_eu_dwh"."shopify_global"."fct_order_enriched" foe
           WHERE date(foe.created_at) >= (current_date - 60)
        UNION
         SELECT NULL::character varying AS country
        ), warehouse_list AS (
         SELECT DISTINCT warehouse_id
           FROM "airup_eu_dwh"."odoo"."sale_order" so
          WHERE so.create_date::date >= (current_date - 60)
        ), warehouse_country as
(SELECT country_list.country,
    t1.warehouse_id
   FROM country_list
     CROSS JOIN ( SELECT warehouse_list.warehouse_id
           FROM warehouse_list
        UNION
         SELECT NULL::text AS warehouse_id) t1),  
        -- , date_series as (select full_date from reports.dates d where full_date >= current_date-60),
 --  hour_series as (select gen_num as hour_in_day from reports.series_of_number son),
        date_series AS (
         SELECT COALESCE(t3.country, 'Other'::character varying) AS shopify_country,
            COALESCE(t3.warehouse_id, 'Other'::text) AS so_warehouse_id,
            t1.create_date,
            t2.hour_in_day
           FROM (select full_date as create_date from "airup_eu_dwh"."reports"."dates" d where full_date >= current_date-60
           and full_date <= current_date) t1
             CROSS JOIN ( select gen_num as hour_in_day from "airup_eu_dwh"."reports"."series_of_number" where gen_num<=23) t2
             CROSS JOIN ( SELECT warehouse_country.country,
                    warehouse_country.warehouse_id
                   FROM warehouse_country) t3
        )
 SELECT ds.create_date,
    ds.hour_in_day,
    ds.so_warehouse_id,
    ds.shopify_country,
    igh.number_of_order_iqi,
    igh2.number_of_order_ie
   FROM date_series ds
     LEFT JOIN iqi_get_hour igh ON ds.create_date = igh.iqi_create_date AND ds.hour_in_day::double precision = igh.hour_in_day AND ds.shopify_country::text = igh.shopify_country::text AND ds.so_warehouse_id = igh.so_warehouse_id
     LEFT JOIN ie_get_hour igh2 ON ds.create_date = igh2.ie_create_date 
     AND ds.hour_in_day::double precision = igh2.hour_in_day 
     AND ds.shopify_country::text = igh2.shopify_country::text AND ds.so_warehouse_id = igh2.so_warehouse_id