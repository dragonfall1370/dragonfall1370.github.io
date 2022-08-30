

WITH google_ads AS (
         SELECT ga.date,
            ga.country_grouping,
            ga.country_fullname,
            ga.account_id,
            ga.channel,
            ga.source,
            ga.ad_id,
            ga.campaign_name,
            ga.campaign_type,
            ga.impressions,
            ga.clicks,
            ga.cost,
            ga.conversions,
            ga.conversion_value
           FROM "airup_eu_dwh"."adwords"."fct_daily_google_sea_display_ads_by_campaigns" ga
        ), bing_ads AS (
         SELECT ba.date,
            ba.country_grouping,
            ba.country_fullname,
            ba.account_id,
            ba.channel,
            ba.source,
            ba.ad_id,
            ba.campaign_name,
            ba.campaign_type,
            ba.impressions,
            ba.clicks,
            ba.cost,
            ba.conversions,
            ba.conversion_value,
            ba.orders
           FROM "airup_eu_dwh"."bingads"."fct_daily_bing_ads_by_campaigns" ba
        )
 SELECT google_ads.date,
    google_ads.country_grouping,
    google_ads.country_fullname,
    google_ads.account_id,
    google_ads.channel,
    google_ads.source,
    google_ads.ad_id,
    google_ads.campaign_name,
    google_ads.campaign_type,
    google_ads.impressions,
    google_ads.clicks,
    google_ads.cost,
    google_ads.conversions,
    google_ads.conversion_value
   FROM google_ads
UNION
 SELECT bing_ads.date,
    bing_ads.country_grouping AS country_grouping,
    bing_ads.country_fullname,
    bing_ads.account_id,
    bing_ads.channel,
    bing_ads.source,
    bing_ads.ad_id,
    bing_ads.campaign_name,
    bing_ads.campaign_type,
    bing_ads.impressions,
    bing_ads.clicks,
    bing_ads.cost,
    bing_ads.conversions,
    bing_ads.conversion_value
   FROM bing_ads
  ORDER BY 1 DESC