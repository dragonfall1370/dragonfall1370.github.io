


SELECT ccvra.date,
        CASE
            WHEN "right"(ccvra.ad_id::text, 3) = '774'::text THEN 'Austria'::text
            WHEN "right"(ccvra.ad_id::text, 3) = '786'::text THEN 'Germany'::text
            WHEN "right"(ccvra.ad_id::text, 3) = '706'::text THEN 'France'::text
            WHEN "right"(ccvra.ad_id::text, 3) = '611'::text THEN 'Netherlands'::text
            WHEN "right"(ccvra.ad_id::text, 3) = '705'::text THEN 'United Kingdom'::text
            WHEN "right"(ccvra.ad_id::text, 3) = '085'::text THEN 'Switzerland'::text
            WHEN "right"(ccvra.ad_id::text, 3) = '033'::text THEN 'Italy'::text
            WHEN "right"(ccvra.ad_id::text, 3) = '046'::text THEN 'Sweden'::text
            WHEN "right"(ccvra.ad_id::text, 3) = '271'::text THEN 'Belgium'::text 
            WHEN "right"(ccvra.ad_id::text, 3) = '652'::text THEN 'United States'::text
            ELSE NULL::text
        END AS country_fullname,
        CASE
            WHEN "right"(ccvra.ad_id::text, 3) = '774'::text THEN 'AT'::text
            WHEN "right"(ccvra.ad_id::text, 3) = '786'::text THEN 'DE'::text
            WHEN "right"(ccvra.ad_id::text, 3) = '706'::text THEN 'FR'::text
            WHEN "right"(ccvra.ad_id::text, 3) = '611'::text THEN 'NL'::text
            WHEN "right"(ccvra.ad_id::text, 3) = '705'::text THEN 'UK'::text
            WHEN "right"(ccvra.ad_id::text, 3) = '085'::text THEN 'CH'::text
            WHEN "right"(ccvra.ad_id::text, 3) = '033'::text THEN 'IT'::text
            WHEN "right"(ccvra.ad_id::text, 3) = '046'::text THEN 'SE'::text
            WHEN "right"(ccvra.ad_id::text, 3) = '271'::text THEN 'BE'::text
            WHEN "right"(ccvra.ad_id::text, 3) = '652'::text THEN 'US'::text
            ELSE NULL::text
        END AS region,
    ccvra.ad_id,
    sum(ccvra.value) AS orders
   FROM  "airup_eu_dwh"."facebook_ads"."custom_conversion_report_actions" ccvra
  WHERE ccvra.action_type::text = 'purchase'::text
  GROUP BY 1,2,3,4
  ORDER BY ccvra.date DESC