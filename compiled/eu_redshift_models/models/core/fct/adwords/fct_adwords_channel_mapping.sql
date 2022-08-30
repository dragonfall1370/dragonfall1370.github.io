---Authors: YuShih Hsieh
---Last Modified by: YuShih Hsieh




SELECT ap.campaign_name,
        CASE
            WHEN ap.campaign_name::text ~~ '%_a-shopping_%'::text OR ap.campaign_name::text ~~ '%_a-search_%'::text THEN 'SEA'::text
            WHEN ap.campaign_name::text ~~ '%_a-display_%'::text OR ap.campaign_name::text ~~ '%_a-discovery_%'::text THEN 'Display'::text
            WHEN ap.campaign_name::text ~~ '%_a-video_%'::text THEN 'Video'::text
            ELSE 'Other'::text
        END AS channel
   FROM "airup_eu_dwh"."adwords_new_api"."ad_performance" ap
  GROUP BY ap.campaign_name