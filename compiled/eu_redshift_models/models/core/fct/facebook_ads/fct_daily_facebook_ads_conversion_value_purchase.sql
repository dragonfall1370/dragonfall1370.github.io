

SELECT custom_conversion_value_report_action_values.date,
        CASE
            WHEN "right"(custom_conversion_value_report_action_values.ad_id::text, 3) = '774'::text THEN 'Austria'::text
            WHEN "right"(custom_conversion_value_report_action_values.ad_id::text, 3) = '786'::text THEN 'Germany'::text
            WHEN "right"(custom_conversion_value_report_action_values.ad_id::text, 3) = '706'::text THEN 'France'::text
            WHEN "right"(custom_conversion_value_report_action_values.ad_id::text, 3) = '611'::text THEN 'Netherlands'::text
            WHEN "right"(custom_conversion_value_report_action_values.ad_id::text, 3) = '705'::text THEN 'United Kingdom'::text
            WHEN "right"(custom_conversion_value_report_action_values.ad_id::text, 3) = '085'::text THEN 'Switzerland'::text
            WHEN "right"(custom_conversion_value_report_action_values.ad_id::text, 3) = '033'::text THEN 'Italy'::text
            WHEN "right"(custom_conversion_value_report_action_values.ad_id::text, 3) = '046'::text THEN 'Sweden'::text
            WHEN "right"(custom_conversion_value_report_action_values.ad_id::text, 3) = '271'::text THEN 'Belgium'::text        
            WHEN "right"(custom_conversion_value_report_action_values.ad_id::text, 3) = '652'::text THEN 'United States'::text                
            ELSE NULL::text
        END AS country_fullname,
        CASE
            WHEN "right"(custom_conversion_value_report_action_values.ad_id::text, 3) = '774'::text THEN 'AT'::text
            WHEN "right"(custom_conversion_value_report_action_values.ad_id::text, 3) = '786'::text THEN 'DE'::text
            WHEN "right"(custom_conversion_value_report_action_values.ad_id::text, 3) = '706'::text THEN 'FR'::text
            WHEN "right"(custom_conversion_value_report_action_values.ad_id::text, 3) = '611'::text THEN 'NL'::text
            WHEN "right"(custom_conversion_value_report_action_values.ad_id::text, 3) = '705'::text THEN 'UK'::text
            WHEN "right"(custom_conversion_value_report_action_values.ad_id::text, 3) = '085'::text THEN 'CH'::text
            WHEN "right"(custom_conversion_value_report_action_values.ad_id::text, 3) = '033'::text THEN 'IT'::text
            WHEN "right"(custom_conversion_value_report_action_values.ad_id::text, 3) = '046'::text THEN 'SE'::text
            WHEN "right"(custom_conversion_value_report_action_values.ad_id::text, 3) = '271'::text THEN 'BE'::text 
            WHEN "right"(custom_conversion_value_report_action_values.ad_id::text, 3) = '652'::text THEN 'US'::text               
            ELSE NULL::text
        END AS region,
    custom_conversion_value_report_action_values.ad_id,
    sum(custom_conversion_value_report_action_values.value) AS conversion_value_purchases
   FROM  "airup_eu_dwh"."facebook_ads"."custom_conversion_report_action_values" custom_conversion_value_report_action_values
  WHERE custom_conversion_value_report_action_values.action_type::text = 'offsite_conversion.fb_pixel_purchase'::text
  GROUP BY 1,2,3,4
  order by 1 desc