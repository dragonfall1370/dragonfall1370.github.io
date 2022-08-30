

SELECT visits.day,
    COALESCE(country_system_account_mapping.country_fullname, 'other'::character varying) AS country,
    COALESCE(country_system_account_mapping.country_grouping, 'other'::character varying) AS region,
    visits.ua_form_factor AS device_type,
    sum(visits.total_sessions) AS sessions,
    sum(visits.total_visitors) AS visitors,
    sum(COALESCE(visits.total_orders_placed, 0))::double precision / sum(COALESCE(visits.total_sessions, 0))::double precision AS conversion_rate
   FROM "airup_eu_dwh"."shopify_visits"."visits" visits
     LEFT JOIN "airup_eu_dwh"."public"."country_system_account_mapping" country_system_account_mapping ON visits.location_country::text = country_system_account_mapping.country_fullname::text
  GROUP BY visits.day, (COALESCE(country_system_account_mapping.country_fullname, 'other'::character varying)), (COALESCE(country_system_account_mapping.country_grouping, 'other'::character varying)), visits.ua_form_factor