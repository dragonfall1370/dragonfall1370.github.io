

WITH campaign AS (
         SELECT cp.date,
            am.country_fullname,
            am.country_grouping,
            am.country_abbreviation,
            cp.account_id,
            'SEA'::text AS channel,
            'Bing'::text AS source,
            cp.campaign_id::character varying(256) AS ad_id,
            cp.campaign_name,
                CASE
                    WHEN cp.campaign_name::text ~~ '%-generic_%'::text THEN 'Generic'::text
                    WHEN cp.campaign_name::text ~~ '%-retention_%'::text AND cp.campaign_name::text ~~ '%_a-video_%'::text THEN 'Customer'::text
                    WHEN cp.campaign_name::text ~~ '%-retention_%'::text AND (cp.campaign_name::text ~~ '%_a-display_%'::text OR cp.campaign_name::text ~~ '%_a-discovery_%'::text) THEN 'Retention'::text
                    WHEN cp.campaign_name::text ~~ '%-cus_%'::text THEN 'Customer'::text
                    WHEN cp.campaign_name::text ~~ '%-retargeting_%'::text OR cp.campaign_name::text ~~ '%-ret_%'::text THEN 'Retargeting'::text
                    WHEN cp.campaign_name::text ~~ '%-brand_%'::text THEN 'Brand'::text
                    WHEN cp.campaign_name::text ~~ '%-prospecting_%'::text OR cp.campaign_name::text ~~ '%-pro_%'::text THEN 'Prospecting'::text
                    ELSE 'Other'::text
                END AS campaign_type,
            sum(cp.impressions) AS impressions,
            sum(cp.clicks) AS clicks,
            sum(cp.spend) AS cost,
            sum(cp.conversions)::double precision AS conversions,
            sum(cp.revenue) AS conversion_value
           FROM "airup_eu_dwh"."bingads"."campaign_performance_daily_report" cp
             LEFT JOIN "airup_eu_dwh"."bingads"."fct_bing_account_mapping" am ON am.account_id = cp.account_id
          GROUP BY 1,2,3,4,5,6,7,8,9,10
          ORDER BY cp.date DESC
        ), count_order AS (
         SELECT fbam.date,
            fbam.campaign,
            sum(fbam.transactions) AS orders
           FROM "airup_eu_dwh"."google_analytics"."fct_basic_acquisition_metrics" fbam
          GROUP BY fbam.date, fbam.campaign
        )
 SELECT campaign.date,
    campaign.country_fullname,
    campaign.country_grouping,
    campaign.country_abbreviation,
    campaign.account_id,
    campaign.channel,
    campaign.source,
    campaign.ad_id,
    campaign.campaign_name,
    campaign.campaign_type,
    campaign.impressions,
    campaign.clicks,
    campaign.cost,
    campaign.conversions,
    campaign.conversion_value,
    count_order.orders
   FROM campaign
     LEFT JOIN count_order ON campaign.date = count_order.date AND campaign.campaign_name::text = count_order.campaign::text
  ORDER BY campaign.date DESC