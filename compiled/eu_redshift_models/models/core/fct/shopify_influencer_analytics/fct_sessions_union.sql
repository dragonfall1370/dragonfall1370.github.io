

WITH airup_gmbh_sessions_raw AS (
         SELECT airup_gmbh_visits.day,
            airup_gmbh_visits.location_country AS country_of_session,
            airup_gmbh_visits.utm_campaign_medium,
            airup_gmbh_visits.utm_campaign_name,
            airup_gmbh_visits.total_visitors AS users,
            airup_gmbh_visits.total_sessions AS sessions,
            airup_gmbh_visits._fivetran_synced AS creation_date,
            'Base'::text AS shopify_shop
           FROM "airup_eu_dwh"."shopify_influencer_analytics"."airup_gmbh_visits" airup_gmbh_visits
        ), airup_gmbh_sessions_final AS (
         SELECT airup_gmbh_sessions_raw.day,
            airup_gmbh_sessions_raw.country_of_session,
            airup_gmbh_sessions_raw.utm_campaign_medium,
            airup_gmbh_sessions_raw.utm_campaign_name,
            airup_gmbh_sessions_raw.users,
            airup_gmbh_sessions_raw.sessions,
            airup_gmbh_sessions_raw.creation_date,
            airup_gmbh_sessions_raw.shopify_shop
           FROM airup_gmbh_sessions_raw
        ), airup_ch_sessions_raw AS (
         SELECT airup_ch_visits.day,
            airup_ch_visits.location_country AS country_of_session,
            airup_ch_visits.utm_campaign_medium,
            airup_ch_visits.utm_campaign_name,
            airup_ch_visits.total_visitors AS users,
            airup_ch_visits.total_sessions AS sessions,
            airup_ch_visits._fivetran_synced AS creation_date,
            'CH'::text AS shopify_shop
           FROM "airup_eu_dwh"."shopify_influencer_analytics"."airup_ch_visits" airup_ch_visits
        ), airup_ch_sessions_final AS (
         SELECT airup_ch_sessions_raw.day,
            airup_ch_sessions_raw.country_of_session,
            airup_ch_sessions_raw.utm_campaign_medium,
            airup_ch_sessions_raw.utm_campaign_name,
            airup_ch_sessions_raw.users,
            airup_ch_sessions_raw.sessions,
            airup_ch_sessions_raw.creation_date,
            airup_ch_sessions_raw.shopify_shop
           FROM airup_ch_sessions_raw
        ), airup_uk_sessions_raw AS (
         SELECT airup_uk_visits.day,
            airup_uk_visits.location_country AS country_of_session,
            airup_uk_visits.utm_campaign_medium,
            airup_uk_visits.utm_campaign_name,
            airup_uk_visits.total_visitors AS users,
            airup_uk_visits.total_sessions AS sessions,
            airup_uk_visits._fivetran_synced AS creation_date,
            'UK'::text AS shopify_shop
           FROM "airup_eu_dwh"."shopify_influencer_analytics"."airup_uk_visits" airup_uk_visits
        ), airup_uk_sessions_final AS (
         SELECT airup_uk_sessions_raw.day,
            airup_uk_sessions_raw.country_of_session,
            airup_uk_sessions_raw.utm_campaign_medium,
            airup_uk_sessions_raw.utm_campaign_name,
            airup_uk_sessions_raw.users,
            airup_uk_sessions_raw.sessions,
            airup_uk_sessions_raw.creation_date,
            airup_uk_sessions_raw.shopify_shop
           FROM airup_uk_sessions_raw
        ), airup_fr_sessions_raw AS (
         SELECT airup_fr_visits.day,
            airup_fr_visits.location_country AS country_of_session,
            airup_fr_visits.utm_campaign_medium,
            airup_fr_visits.utm_campaign_name,
            airup_fr_visits.total_visitors AS users,
            airup_fr_visits.total_sessions AS sessions,
            airup_fr_visits._fivetran_synced AS creation_date,
            'FR'::text AS shopify_shop
           FROM "airup_eu_dwh"."shopify_influencer_analytics"."airup_fr_visits" airup_fr_visits
        ), airup_fr_sessions_final AS (
         SELECT airup_fr_sessions_raw.day,
            airup_fr_sessions_raw.country_of_session,
            airup_fr_sessions_raw.utm_campaign_medium,
            airup_fr_sessions_raw.utm_campaign_name,
            airup_fr_sessions_raw.users,
            airup_fr_sessions_raw.sessions,
            airup_fr_sessions_raw.creation_date,
            airup_fr_sessions_raw.shopify_shop
           FROM airup_fr_sessions_raw
        ), airup_nl_sessions_raw AS (
         SELECT airup_nl_visits.day,
            airup_nl_visits.location_country AS country_of_session,
            airup_nl_visits.utm_campaign_medium,
            airup_nl_visits.utm_campaign_name,
            airup_nl_visits.total_visitors AS users,
            airup_nl_visits.total_sessions AS sessions,
            airup_nl_visits._fivetran_synced AS creation_date,
            'NL'::text AS shopify_shop
           FROM "airup_eu_dwh"."shopify_influencer_analytics"."airup_nl_visits" airup_nl_visits
        ), airup_nl_sessions_final AS (
         SELECT airup_nl_sessions_raw.day,
            airup_nl_sessions_raw.country_of_session,
            airup_nl_sessions_raw.utm_campaign_medium,
            airup_nl_sessions_raw.utm_campaign_name,
            airup_nl_sessions_raw.users,
            airup_nl_sessions_raw.sessions,
            airup_nl_sessions_raw.creation_date,
            airup_nl_sessions_raw.shopify_shop
           FROM airup_nl_sessions_raw
        ), airup_it_sessions_raw AS (
         SELECT airup_it_visits.day,
            airup_it_visits.location_country AS country_of_session,
            airup_it_visits.utm_campaign_medium,
            airup_it_visits.utm_campaign_name,
            airup_it_visits.total_visitors AS users,
            airup_it_visits.total_sessions AS sessions,
            airup_it_visits._fivetran_synced AS creation_date,
            'IT'::text AS shopify_shop
           FROM "airup_eu_dwh"."shopify_influencer_analytics"."airup_it_visits" airup_it_visits
        ), airup_it_sessions_final AS (
         SELECT airup_it_sessions_raw.day,
            airup_it_sessions_raw.country_of_session,
            airup_it_sessions_raw.utm_campaign_medium,
            airup_it_sessions_raw.utm_campaign_name,
            airup_it_sessions_raw.users,
            airup_it_sessions_raw.sessions,
            airup_it_sessions_raw.creation_date,
            airup_it_sessions_raw.shopify_shop
           FROM airup_it_sessions_raw
        ), airup_se_sessions_raw AS (
         SELECT airup_se_visits.day,
            airup_se_visits.location_country AS country_of_session,
            airup_se_visits.utm_campaign_medium,
            airup_se_visits.utm_campaign_name,
            airup_se_visits.total_visitors AS users,
            airup_se_visits.total_sessions AS sessions,
            airup_se_visits._fivetran_synced AS creation_date,
            'IT'::text AS shopify_shop
           FROM "airup_eu_dwh"."shopify_influencer_analytics"."airup_se_visits" airup_se_visits
        ), airup_se_sessions_final AS (
         SELECT airup_se_sessions_raw.day,
            airup_se_sessions_raw.country_of_session,
            airup_se_sessions_raw.utm_campaign_medium,
            airup_se_sessions_raw.utm_campaign_name,
            airup_se_sessions_raw.users,
            airup_se_sessions_raw.sessions,
            airup_se_sessions_raw.creation_date,
            airup_se_sessions_raw.shopify_shop
           FROM airup_se_sessions_raw
        )
 SELECT airup_gmbh_sessions_final.day,
    airup_gmbh_sessions_final.country_of_session,
    airup_gmbh_sessions_final.utm_campaign_medium,
    airup_gmbh_sessions_final.utm_campaign_name,
    airup_gmbh_sessions_final.users,
    airup_gmbh_sessions_final.sessions,
    airup_gmbh_sessions_final.creation_date,
    airup_gmbh_sessions_final.shopify_shop
   FROM airup_gmbh_sessions_final
UNION ALL
 SELECT airup_ch_sessions_final.day,
    airup_ch_sessions_final.country_of_session,
    airup_ch_sessions_final.utm_campaign_medium,
    airup_ch_sessions_final.utm_campaign_name,
    airup_ch_sessions_final.users,
    airup_ch_sessions_final.sessions,
    airup_ch_sessions_final.creation_date,
    airup_ch_sessions_final.shopify_shop
   FROM airup_ch_sessions_final
UNION ALL
 SELECT airup_uk_sessions_final.day,
    airup_uk_sessions_final.country_of_session,
    airup_uk_sessions_final.utm_campaign_medium,
    airup_uk_sessions_final.utm_campaign_name,
    airup_uk_sessions_final.users,
    airup_uk_sessions_final.sessions,
    airup_uk_sessions_final.creation_date,
    airup_uk_sessions_final.shopify_shop
   FROM airup_uk_sessions_final
UNION ALL
 SELECT airup_fr_sessions_final.day,
    airup_fr_sessions_final.country_of_session,
    airup_fr_sessions_final.utm_campaign_medium,
    airup_fr_sessions_final.utm_campaign_name,
    airup_fr_sessions_final.users,
    airup_fr_sessions_final.sessions,
    airup_fr_sessions_final.creation_date,
    airup_fr_sessions_final.shopify_shop
   FROM airup_fr_sessions_final
UNION ALL
 SELECT airup_nl_sessions_final.day,
    airup_nl_sessions_final.country_of_session,
    airup_nl_sessions_final.utm_campaign_medium,
    airup_nl_sessions_final.utm_campaign_name,
    airup_nl_sessions_final.users,
    airup_nl_sessions_final.sessions,
    airup_nl_sessions_final.creation_date,
    airup_nl_sessions_final.shopify_shop
   FROM airup_nl_sessions_final
UNION ALL
 SELECT airup_it_sessions_final.day,
    airup_it_sessions_final.country_of_session,
    airup_it_sessions_final.utm_campaign_medium,
    airup_it_sessions_final.utm_campaign_name,
    airup_it_sessions_final.users,
    airup_it_sessions_final.sessions,
    airup_it_sessions_final.creation_date,
    airup_it_sessions_final.shopify_shop
   FROM airup_it_sessions_final
UNION ALL
 SELECT airup_se_sessions_final.day,
    airup_se_sessions_final.country_of_session,
    airup_se_sessions_final.utm_campaign_medium,
    airup_se_sessions_final.utm_campaign_name,
    airup_se_sessions_final.users,
    airup_se_sessions_final.sessions,
    airup_se_sessions_final.creation_date,
    airup_se_sessions_final.shopify_shop
   FROM airup_se_sessions_final