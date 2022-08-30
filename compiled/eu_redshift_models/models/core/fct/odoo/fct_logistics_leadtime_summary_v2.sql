--- creator: Nham Dao
--- last modify: Nham Dao
--- summary: summary of orders in each step (shopify, odoo, warehouse, warehouse tracking)


SELECT date(fct_logistics_leadtime_data.shopify_create_date) AS date_timestamp,
    'Shopify order'::text AS source_information,
    fct_logistics_leadtime_data.shopify_country AS country,
    count(fct_logistics_leadtime_data.shopify_order_number) AS number_of_order,
    NULL::character varying AS sp_sale_teams,
    NULL::text AS sp_state
   FROM "airup_eu_dwh"."odoo"."fct_logistics_leadtime_data"
  WHERE date(fct_logistics_leadtime_data.shopify_create_date) IS NOT NULL
  GROUP BY date(fct_logistics_leadtime_data.shopify_create_date), fct_logistics_leadtime_data.shopify_country
UNION
 SELECT date(fct_logistics_leadtime_data.sp_create_date) AS date_timestamp,
    'Stock picking'::text AS source_information,
    fct_logistics_leadtime_data.shopify_country AS country,
    count(fct_logistics_leadtime_data.sp_name) AS number_of_order,
    fct_logistics_leadtime_data.sp_sale_teams,
    NULL::text AS sp_state
   FROM  "airup_eu_dwh"."odoo"."fct_logistics_leadtime_data"
  WHERE date(fct_logistics_leadtime_data.sp_create_date) IS NOT NULL AND (fct_logistics_leadtime_data.sp_sale_teams::text = ANY (ARRAY['D2C'::character varying::text, 'D2C - CH'::character varying::text, 'D2C - FR'::character varying::text, 'D2C - IT'::character varying::text, 'D2C - NL'::character varying::text, 'D2C - UK'::character varying::text, 'Influencer'::character varying::text])) AND fct_logistics_leadtime_data.sp_state::text <> 'cancel'::text
  GROUP BY date(fct_logistics_leadtime_data.sp_create_date), fct_logistics_leadtime_data.shopify_country, fct_logistics_leadtime_data.sp_sale_teams
UNION
 SELECT date(fct_logistics_leadtime_data.ie_create_date) AS date_timestamp,
    'Warehouse order'::text AS source_information,
    fct_logistics_leadtime_data.shopify_country AS country,
    count(fct_logistics_leadtime_data.ie_create_date) AS number_of_order,
    NULL::character varying AS sp_sale_teams,
    NULL::text AS sp_state
   FROM  "airup_eu_dwh"."odoo"."fct_logistics_leadtime_data"
  WHERE date(fct_logistics_leadtime_data.ie_create_date) IS NOT NULL
  GROUP BY date(fct_logistics_leadtime_data.ie_create_date), fct_logistics_leadtime_data.shopify_country
UNION
 SELECT date(fct_logistics_leadtime_data.iqi_create_date) AS date_timestamp,
    'Tracking from whs'::text AS source_information,
    fct_logistics_leadtime_data.shopify_country AS country,
    count(fct_logistics_leadtime_data.iqi_name_ref) AS number_of_order,
    NULL::character varying AS sp_sale_teams,
    NULL::text AS sp_state
   FROM  "airup_eu_dwh"."odoo"."fct_logistics_leadtime_data"
  WHERE date(fct_logistics_leadtime_data.iqi_create_date) IS NOT NULL
  GROUP BY date(fct_logistics_leadtime_data.iqi_create_date), fct_logistics_leadtime_data.shopify_country
UNION
 SELECT date(fct_logistics_leadtime_data.sp_create_date) AS date_timestamp,
    'Open Order (Odoo)'::text AS source_information,
    fct_logistics_leadtime_data.shopify_country AS country,
    sum(
        CASE
            WHEN fct_logistics_leadtime_data.sp_name IS NOT NULL AND fct_logistics_leadtime_data.iqi_name_ref IS NULL THEN 1
            ELSE 0
        END) AS number_of_order,
    NULL::character varying AS sp_sale_teams,
        CASE
            WHEN fct_logistics_leadtime_data.sp_state::text = 'confirmed'::text THEN 'Waiting'::text
            WHEN fct_logistics_leadtime_data.sp_state::text = 'assigned'::text THEN 'Ready'::text
            ELSE 'Others'::text
        END AS sp_state
   FROM  "airup_eu_dwh"."odoo"."fct_logistics_leadtime_data"
  WHERE date(fct_logistics_leadtime_data.sp_create_date) IS NOT NULL AND date(fct_logistics_leadtime_data.iqi_create_date) IS NULL AND (fct_logistics_leadtime_data.sp_sale_teams::text = ANY (ARRAY['D2C'::character varying::text, 'D2C - CH'::character varying::text, 'D2C - FR'::character varying::text, 'D2C - IT'::character varying::text, 'D2C - NL'::character varying::text, 'D2C - UK'::character varying::text, 'Influencer'::character varying::text])) AND (fct_logistics_leadtime_data.sp_state::text = ANY (ARRAY['confirmed'::character varying::text, 'assigned'::character varying::text])) AND fct_logistics_leadtime_data.sp_carrier_tracking_ref = 'false'::text
  GROUP BY date(fct_logistics_leadtime_data.sp_create_date), fct_logistics_leadtime_data.shopify_country, fct_logistics_leadtime_data.sp_state