

 SELECT ad_account_history.id,
         CASE
             WHEN ad_account_history.id::text = 'b3461e1a-4e8a-4344-ae84-aabf1b3b996d'::text THEN 'DE'::text
            WHEN ad_account_history.id::text = '8f36d914-6815-42cb-b7c5-d59f496b4954'::text THEN 'IT'::text
            WHEN ad_account_history.id::text = 'a41347c2-6727-4e9e-be07-ff957b704dfb'::text THEN 'CH'::text
            WHEN ad_account_history.id::text = '696c5daa-e68f-40b8-bc6d-4169a6840159'::text THEN 'NL'::text
            WHEN ad_account_history.id::text = 'f4ee80c6-419e-4747-ac39-1ca912c7c21f'::text THEN 'UK'::text
            WHEN ad_account_history.id::text = 'ce2dcd64-5d8c-4af6-a83a-be3dbf4acc01'::text THEN 'SE'::text
            ELSE NULL::text
        END AS country,
         CASE
            WHEN ad_account_history.id::text = 'b3461e1a-4e8a-4344-ae84-aabf1b3b996d'::text THEN 'Germany'::text
            WHEN ad_account_history.id::text = '8f36d914-6815-42cb-b7c5-d59f496b4954'::text THEN 'Italy'::text
            WHEN ad_account_history.id::text = 'a41347c2-6727-4e9e-be07-ff957b704dfb'::text THEN 'Switzerland'::text
            WHEN ad_account_history.id::text = '696c5daa-e68f-40b8-bc6d-4169a6840159'::text THEN 'Netherlands'::text
            WHEN ad_account_history.id::text = 'f4ee80c6-419e-4747-ac39-1ca912c7c21f'::text THEN 'United Kingdom'::text
            WHEN ad_account_history.id::text = 'ce2dcd64-5d8c-4af6-a83a-be3dbf4acc01'::text THEN 'Sweden'::text
            ELSE NULL::text
        END AS country_fullname,
        CASE
            WHEN ad_account_history.id::text = 'b3461e1a-4e8a-4344-ae84-aabf1b3b996d'::text or  ad_account_history.id::text = 'a41347c2-6727-4e9e-be07-ff957b704dfb'::text THEN 'Central Europe'::text
            WHEN ad_account_history.id::text = '8f36d914-6815-42cb-b7c5-d59f496b4954'::text THEN 'South Europe'::text
            WHEN ad_account_history.id::text = '696c5daa-e68f-40b8-bc6d-4169a6840159'::text or ad_account_history.id::text = 'f4ee80c6-419e-4747-ac39-1ca912c7c21f'::text or ad_account_history.id::text = 'ce2dcd64-5d8c-4af6-a83a-be3dbf4acc01'::text THEN 'North Europe'::text
            ELSE NULL::text
        END AS country_grouping
   FROM "airup_eu_dwh"."snapchat_ads"."ad_account_history" ad_account_history
  GROUP BY ad_account_history.id