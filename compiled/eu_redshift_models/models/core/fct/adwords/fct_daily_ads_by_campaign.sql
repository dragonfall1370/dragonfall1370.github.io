

WITH google_ads AS (
         SELECT daily_google_ads_by_campaigns.date,
            daily_google_ads_by_campaigns.region,
            daily_google_ads_by_campaigns.account_id,
            daily_google_ads_by_campaigns.channel,
            daily_google_ads_by_campaigns.ad_id,
            daily_google_ads_by_campaigns.campaign_name,
            daily_google_ads_by_campaigns.campaign_type,
            daily_google_ads_by_campaigns.conversions,
            daily_google_ads_by_campaigns.cost,
            daily_google_ads_by_campaigns.orders
           FROM "airup_eu_dwh"."adwords"."fct_daily_google_ads_by_campaigns" daily_google_ads_by_campaigns
        ), facebook_ads AS (
         SELECT daily_facebook_ads_by_campaigns.date,
            daily_facebook_ads_by_campaigns.region,
            daily_facebook_ads_by_campaigns.account_id,
            daily_facebook_ads_by_campaigns.channel,
            daily_facebook_ads_by_campaigns.ad_id,
            daily_facebook_ads_by_campaigns.campaign_name,
            daily_facebook_ads_by_campaigns.campaign_type,
            daily_facebook_ads_by_campaigns.conversions,
            daily_facebook_ads_by_campaigns.cost,
            daily_facebook_ads_by_campaigns.orders
           FROM "airup_eu_dwh"."facebook_ads"."fct_daily_facebook_ads_by_campaigns" daily_facebook_ads_by_campaigns
        ), bing_ads AS (
         SELECT daily_bing_ads_by_campaigns.date,
            daily_bing_ads_by_campaigns.country_abbreviation AS region,
            daily_bing_ads_by_campaigns.account_id,
            daily_bing_ads_by_campaigns.channel,
            daily_bing_ads_by_campaigns.ad_id,
            daily_bing_ads_by_campaigns.campaign_name,
            daily_bing_ads_by_campaigns.campaign_type,
            daily_bing_ads_by_campaigns.conversions,
            daily_bing_ads_by_campaigns.cost,
            daily_bing_ads_by_campaigns.orders
           FROM "airup_eu_dwh"."bingads"."fct_daily_bing_ads_by_campaigns" daily_bing_ads_by_campaigns
        )
 SELECT google_ads.date,
    google_ads.region,
    google_ads.channel,
    google_ads.ad_id,
    google_ads.campaign_name,
    google_ads.campaign_type,
    google_ads.conversions,
    google_ads.cost,
    google_ads.orders
   FROM google_ads
UNION
 SELECT facebook_ads.date,
    facebook_ads.region,
    facebook_ads.channel,
    facebook_ads.ad_id,
    facebook_ads.campaign_name,
    facebook_ads.campaign_type,
    facebook_ads.conversions,
    facebook_ads.cost,
    facebook_ads.orders
   FROM facebook_ads
UNION
 SELECT bing_ads.date,
    bing_ads.region,
    bing_ads.channel,
    bing_ads.ad_id,
    bing_ads.campaign_name,
    bing_ads.campaign_type,
    bing_ads.conversions,
    bing_ads.cost,
    bing_ads.orders
   FROM bing_ads
  ORDER BY 1 DESC