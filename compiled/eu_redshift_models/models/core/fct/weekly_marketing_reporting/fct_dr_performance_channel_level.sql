

WITH media_spend AS (
         SELECT date,
            country,
                CASE
                    WHEN media_platform = 'facebook' THEN 'Meta'
                    WHEN media_platform = 'google' AND channel_grouping_lvl2 = 'Youtube' THEN 'YT'
                    WHEN media_platform = 'influencer' THEN 'IM'
                    WHEN media_platform = 'tiktok' THEN 'TK'
                    ELSE 'Other'
                END AS media_platform,
            sum(media_spend) AS media_spend
           FROM "airup_eu_dwh"."weekly_marketing_reporting"."fct_media_spend_by_sales_channel_and_platform" media_spend_by_sales_channel_and_platform
          WHERE media_platform <> 'amazon'
          GROUP BY date, country, (
                CASE
                    WHEN media_platform = 'facebook' THEN 'Meta'
                    WHEN media_platform = 'google' AND channel_grouping_lvl2 = 'Youtube' THEN 'YT'
                    WHEN media_platform = 'influencer' THEN 'IM'
                    WHEN media_platform = 'tiktok' THEN 'TK'
                    ELSE 'Other'
                END)
        ), impressions_clicks AS (
         SELECT custom_report_enriched.date,
            custom_report_enriched.country_fullname AS country,
                CASE
                    WHEN ad_network_type = 'YOUTUBE_WATCH' AND "name" ~~ '%a-video%' THEN 'YT'
                    ELSE 'Other'
                END AS media_platform,
            sum(custom_report_enriched.impressions) AS impressions,
            sum(custom_report_enriched.clicks) AS clicks
           FROM "airup_eu_dwh"."adwords"."fct_custom_report_enriched" custom_report_enriched
          GROUP BY custom_report_enriched.date, custom_report_enriched.country_fullname, (
                CASE
                    WHEN ad_network_type = 'YOUTUBE_WATCH' AND "name" ~~ '%a-video%' THEN 'YT'
                    ELSE 'Other'
                END)
        UNION ALL
         SELECT fct_custom_conversion_value_report_enriched.date,
            country_system_account_mapping.country_fullname AS country,
            'Meta'::text AS media_platform,
            sum(fct_custom_conversion_value_report_enriched.impressions) AS impressions,
            sum(fct_custom_conversion_value_report_enriched.inline_link_clicks) AS clicks
           FROM "airup_eu_dwh"."facebook_ads"."fct_custom_conversion_value_report_enriched" fct_custom_conversion_value_report_enriched
             LEFT JOIN "airup_eu_dwh"."public"."country_system_account_mapping" country_system_account_mapping ON fct_custom_conversion_value_report_enriched.account_id = ltrim(rtrim(country_system_account_mapping.facebook_account_ids , '}'), '{')::int8
          GROUP BY fct_custom_conversion_value_report_enriched.date, country_system_account_mapping.country_fullname, 'Meta'::text
        UNION ALL
         SELECT influencer_revenues_posting_date.date,
            influencer_revenues_posting_date.country,
            'IM'::text AS media_platform,
            COALESCE(sum(influencer_revenues_posting_date.r_views), 0::numeric) AS impressions,
            COALESCE(sum(influencer_revenues_posting_date.sessions), 0::double precision) AS clicks
           FROM "airup_eu_dwh"."reports"."influencer_revenues_posting_date" influencer_revenues_posting_date
          GROUP BY influencer_revenues_posting_date.date, influencer_revenues_posting_date.country, 'IM'::text
        UNION ALL
        SELECT ad_country_report.stat_time_day AS "date",
            country_system_account_mapping.country_fullname AS country,
            'TK'::text AS media_platform,
            COALESCE(sum(ad_country_report.impressions), 0::numeric) AS impressions,
            COALESCE(sum(ad_country_report.clicks), 0::double precision) AS clicks
           FROM "airup_eu_dwh"."tiktok_ads"."ad_country_report" ad_country_report
           LEFT JOIN "airup_eu_dwh"."public"."country_system_account_mapping" country_system_account_mapping ON ad_country_report.country_code = country_system_account_mapping.country_abbreviation
          GROUP BY ad_country_report.stat_time_day, country_system_account_mapping.country_fullname, 'TK'::text
          
        )
 SELECT media_spend.date,
    media_spend.country,
    media_spend.media_platform,
    sum(media_spend.media_spend) AS media_spend,
    sum(impressions_clicks.impressions) AS impressions,
    sum(impressions_clicks.clicks) AS clicks
   FROM media_spend
     LEFT JOIN impressions_clicks ON media_spend.country = impressions_clicks.country AND media_spend.date = impressions_clicks.date AND media_spend.media_platform = impressions_clicks.media_platform
  GROUP BY media_spend.date, media_spend.country, media_spend.media_platform