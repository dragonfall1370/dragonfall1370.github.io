

with NS as (
    
    select 1 as n  union all 
    
    select 2 as n  union all 
    
    select 3 as n  union all 
    
    select 4 as n  union all 
    
    select 5 as n  union all 
    
    select 6 as n  union all 
    
    select 7 as n  union all 
    
    select 8 as n  union all 
    
    select 9 as n  union all 
    
    select 10 as n  union all 
    
    select 11 as n  union all 
    
    select 12 as n  union all 
    
    select 13 as n  union all 
    
    select 14 as n  union all 
    
    select 15 as n  union all 
    
    select 16 as n  union all 
    
    select 17 as n  union all 
    
    select 18 as n  union all 
    
    select 19 as n  union all 
    
    select 20 as n  union all 
    
    select 21 as n  union all 
    
    select 22 as n  union all 
    
    select 23 as n  union all 
    
    select 24 as n  union all 
    
    select 25 as n  union all 
    
    select 26 as n  union all 
    
    select 27 as n  union all 
    
    select 28 as n  union all 
    
    select 29 as n  union all 
    
    select 30 as n  union all 
    
    select 31 as n  union all 
    
    select 32 as n  union all 
    
    select 33 as n  union all 
    
    select 34 as n  union all 
    
    select 35 as n  union all 
    
    select 36 as n  union all 
    
    select 37 as n  union all 
    
    select 38 as n  union all 
    
    select 39 as n  union all 
    
    select 40 as n  union all 
    
    select 41 as n  union all 
    
    select 42 as n  union all 
    
    select 43 as n  union all 
    
    select 44 as n  union all 
    
    select 45 as n  union all 
    
    select 46 as n  union all 
    
    select 47 as n  union all 
    
    select 48 as n  union all 
    
    select 49 as n 
    
), country_account_mapping as (
    select B.country_fullname, B.country_grouping, B.country_abbreviation,
    TRIM(SPLIT_PART(ltrim(rtrim(B.adwords_customer_ids , '}'), '{'), ',', NS.n))::int8 as "adwords_customer_ids"
    from NS
    inner join "airup_eu_dwh"."public"."country_system_account_mapping" B 
    ON NS.n <= regexp_count(ltrim(rtrim(B.adwords_customer_ids , '}'), '{'), ',') + 1
), campaign AS (
         SELECT ap.date,
            ap.customer_id,
            csam.country_fullname,
            csam.country_grouping,
            csam.country_abbreviation,
            cm.channel,
            'Adwords'::text AS source,
            ap.campaign_id,
            ap.campaign_name,
                CASE
                    WHEN ap.campaign_name::text ~~ '%-generic_%'::text THEN 'Generic'::text
                    WHEN ap.campaign_name::text ~~ '%-retention_%'::text AND ap.campaign_name::text ~~ '%_a-video_%'::text THEN 'Customer'::text
                    WHEN ap.campaign_name::text ~~ '%-retention_%'::text AND (ap.campaign_name::text ~~ '%_a-display_%'::text OR ap.campaign_name::text ~~ '%_a-discovery_%'::text) THEN 'Retention'::text
                    WHEN ap.campaign_name::text ~~ '%-cus_%'::text THEN 'Customer'::text
                    WHEN ap.campaign_name::text ~~ '%-retargeting_%'::text OR ap.campaign_name::text ~~ '%s-ret_%'::text THEN 'Retargeting'::text
                    WHEN ap.campaign_name::text ~~ '%-brand_%'::text THEN 'Brand'::text
                    WHEN ap.campaign_name::text ~~ '%-prospecting_%'::text OR ap.campaign_name::text ~~ '%s-pro_%'::text THEN 'Prospecting'::text
                    ELSE 'Other'::text
                END AS campaign_type,
            sum(ap.impressions) AS impressions,
            sum(ap.clicks) AS clicks,
            sum(ap.cost_micros)/1000000::float8 AS cost,
            sum(ap.conversions) AS conversions,
            sum(ap.conversions_value) AS conversion_value
           FROM "airup_eu_dwh"."adwords_new_api"."ad_performance" ap
             LEFT JOIN country_account_mapping csam ON ap.customer_id in  (ltrim(rtrim(csam.adwords_customer_ids, '}'), '{'))
             LEFT JOIN "airup_eu_dwh"."adwords"."fct_adwords_channel_mapping" cm ON cm.campaign_name::text = ap.campaign_name::text
          WHERE cm.channel = 'SEA'::text OR cm.channel = 'Display'::text
          GROUP BY 1,2,3,4,5,6,7,8,9,10
        )
 SELECT campaign.date,
    campaign.country_grouping,
    campaign.country_fullname,
    campaign.country_abbreviation,
    campaign.customer_id AS account_id,
    campaign.channel,
    campaign.source,
    campaign.campaign_id::character varying(256) AS ad_id,
    campaign.campaign_name,
    campaign.campaign_type,
    campaign.impressions,
    campaign.clicks,
    campaign.cost,
    campaign.conversions,
    campaign.conversion_value
   FROM campaign
  ORDER BY campaign.date DESC