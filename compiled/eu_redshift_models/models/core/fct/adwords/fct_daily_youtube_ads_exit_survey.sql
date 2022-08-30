---Authors: YuShih Hsieh
---Last Modified by: YuShih Hsieh

---###################################################################################################################
        -- this view contains the information on youtube campaign metrics as well as the exit survey event and details
---###################################################################################################################

 

 WITH youtube_cpo AS (
         SELECT yc_1.order_date,
            yc_1.youtube_responses AS "order",
            yc_1.youtube_responses_scaled AS order_sacled,
            yc_1.media_spend,
            yc_1.shipping_country AS region
           FROM "airup_eu_dwh"."exit_survey"."fct_youtube_cpo" yc_1
          WHERE yc_1.shipping_country <> 'All'
        ), youtube_ga AS (
         SELECT daily_youtube_ads_by_campaign.date,
            daily_youtube_ads_by_campaign.region,
            sum(daily_youtube_ads_by_campaign.video_views) AS views,
            sum(daily_youtube_ads_by_campaign.impressions) AS impressions,
            sum(daily_youtube_ads_by_campaign.clicks) AS clicks,
            sum(daily_youtube_ads_by_campaign.cost) AS cost,
            sum(daily_youtube_ads_by_campaign.conversions) AS conversions,
            sum(daily_youtube_ads_by_campaign.conversion_value) AS conversion_value
           FROM  "airup_eu_dwh"."adwords"."fct_daily_youtube_ads_by_campaign" daily_youtube_ads_by_campaign
          GROUP BY daily_youtube_ads_by_campaign.date, daily_youtube_ads_by_campaign.region
        ), youtube_event AS (
         SELECT performance_marketing_youtube_enriched.event_date::date AS event_date,
            performance_marketing_youtube_enriched.region,
            performance_marketing_youtube_enriched.event,
            performance_marketing_youtube_enriched.details
           FROM "airup_eu_dwh"."google_sheets"."fct_performance_marketing_youtube_enriched" performance_marketing_youtube_enriched
        )
 SELECT yg.date,
    yg.region,
    yg.views,
    yg.impressions,
    yg.clicks,
    yg.cost,
    yg.conversions,
    yg.conversion_value,
    yc."order",
    yc.order_sacled,
    yc.media_spend,
    ye.event AS optimization_event,
    ye.details AS optimization_detail
   FROM youtube_ga yg
     LEFT JOIN youtube_cpo yc ON yg.date = yc.order_date AND yg.region = yc.region
     LEFT JOIN youtube_event ye ON yc.order_date = ye.event_date AND yc.region = ye.region
  ORDER BY yg.date DESC