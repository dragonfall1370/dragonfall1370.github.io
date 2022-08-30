---Authors: YuShih Hsieh
---Last Modified by: YuShih Hsieh

---###################################################################################################################
        -- this view contains the information on the event_date, region, event, and details
---###################################################################################################################



SELECT performance_marketing.event_date,
        CASE
            WHEN performance_marketing.country::text = 'Switzerland'::text THEN 'CH'::text
            WHEN performance_marketing.country::text = 'France'::text THEN 'FR'::text
            WHEN performance_marketing.country::text = 'Netherlands'::text THEN 'NL'::text
            WHEN performance_marketing.country::text = 'Italy'::text THEN 'IT'::text
            WHEN performance_marketing.country::text = 'Germany'::text THEN 'DE'::text
            WHEN performance_marketing.country::text = 'Austria'::text THEN 'AT'::text
            WHEN performance_marketing.country::text = 'United Kingdom'::text THEN 'UK'::text
            WHEN performance_marketing.country::text = 'Sweden'::text THEN 'SE'::text
            ELSE 'Other'::text
        END AS region,
    performance_marketing.event,
    performance_marketing.details
   FROM "airup_eu_dwh"."google_sheets"."performance_marketing" performance_marketing
  WHERE performance_marketing.channel::text = 'Youtube'::text
  ORDER BY performance_marketing.event_date DESC