

WITH pa1 AS (
         SELECT visits.day,
                CASE
                    WHEN visits.shop::text = 'gmbh'::text AND visits.day < '2021-07-27'::date AND visits.location_country::text = 'France'::text THEN 'France'::text
                    WHEN visits.shop::text = 'fr'::text THEN 'France'::text
                    WHEN visits.shop::text = 'gmbh'::text AND visits.day < '2021-07-27'::date AND visits.location_country::text = 'Netherlands'::text THEN 'Netherlands'::text
                    WHEN visits.shop::text = 'nl'::text THEN 'Netherlands'::text
                    WHEN visits.shop::text = 'gmbh'::text AND visits.location_country::text <> 'Austria'::text THEN 'Germany'::text
                    WHEN visits.shop::text = 'gmbh'::text AND visits.location_country::text = 'Austria'::text THEN 'Austria'::text
                    WHEN visits.shop::text = 'uk'::text THEN 'United Kingdom'::text
                    WHEN visits.shop::text = 'ch'::text THEN 'Switzerland'::text
                    WHEN visits.shop::text = 'it'::text THEN 'Italy'::text
                    WHEN visits.shop::text = 'se'::text THEN 'Sweden'::text
                    WHEN visits.shop::text = 'at'::text THEN 'Austria'::text
                    ELSE NULL::text
                END AS country,
            visits.ua_form_factor AS device_type,
            sum(visits.total_sessions) AS sessions,
            sum(visits.total_visitors) AS visitors,
            sum(COALESCE(visits.total_orders_placed, 0))::double precision / sum(COALESCE(visits.total_sessions, 0))::double precision AS conversion_rate,
            sum(visits.total_orders_placed) AS orders,
            sum(visits.total_carts) AS add_to_card_clicks,
            sum(visits.total_checkouts) AS checkout_initiation_clicks,
            avg(COALESCE(visits.total_bounce_rate, 0::double precision)) AS bounce_rate,
            sum(
                CASE
                    WHEN visits.page_type::text = 'Product'::text THEN visits.total_sessions
                    ELSE 0
                END) AS pdp_views
           FROM "airup_eu_dwh"."shopify_visits"."visits" visits
          GROUP BY visits.day, (
                CASE
                    WHEN visits.shop::text = 'gmbh'::text AND visits.day < '2021-07-27'::date AND visits.location_country::text = 'France'::text THEN 'France'::text
                    WHEN visits.shop::text = 'fr'::text THEN 'France'::text
                    WHEN visits.shop::text = 'gmbh'::text AND visits.day < '2021-07-27'::date AND visits.location_country::text = 'Netherlands'::text THEN 'Netherlands'::text
                    WHEN visits.shop::text = 'nl'::text THEN 'Netherlands'::text
                    WHEN visits.shop::text = 'gmbh'::text AND visits.location_country::text <> 'Austria'::text THEN 'Germany'::text
                    WHEN visits.shop::text = 'gmbh'::text AND visits.location_country::text = 'Austria'::text THEN 'Austria'::text
                    WHEN visits.shop::text = 'uk'::text THEN 'United Kingdom'::text
                    WHEN visits.shop::text = 'ch'::text THEN 'Switzerland'::text
                    WHEN visits.shop::text = 'it'::text THEN 'Italy'::text
                    ELSE NULL::text
                END), visits.ua_form_factor, visits.shop, visits.location_country
        )
 SELECT pa1.day,
    pa1.country,
    pa1.device_type,
    pa1.sessions,
    pa1.visitors,
    pa1.conversion_rate,
    country_system_account_mapping.country_grouping AS region,
    pa1.orders,
    pa1.add_to_card_clicks,
    pa1.checkout_initiation_clicks,
    pa1.bounce_rate,
    pa1.pdp_views
   FROM pa1
     LEFT JOIN "airup_eu_dwh"."public"."country_system_account_mapping" country_system_account_mapping ON pa1.country = country_system_account_mapping.country_fullname::text