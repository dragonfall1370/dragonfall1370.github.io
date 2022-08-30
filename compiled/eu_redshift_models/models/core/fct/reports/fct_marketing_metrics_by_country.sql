--- Authors: YuShih Hsieh
--- Last Modified by: YuShih Hsieh

--- ###################################################################################################################
        -- this view contains the marketing KPIs from the Adwords, Facebook, Snapchat, TikTok, Bing sources
--- ###################################################################################################################




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
), adwords as (
	select ap.date, csam.country_grouping, csam.country_fullname,
        CASE
            WHEN ap.campaign_name::text ~~ '%_a-shopping_%'::text OR ap.campaign_name::text ~~ '%_a-search_%'::text THEN 'SEA'::text
            WHEN ap.campaign_name::text ~~ '%_a-display_%'::text OR ap.campaign_name::text ~~ '%_a-discovery_%'::text THEN 'Display'::text
            WHEN ap.campaign_name::text ~~ '%_a-video_%'::text THEN 'YouTube'::text
            ELSE 'Other'::text
        END AS channel,
	impressions, cost_micros / 1000000::float8 as cost, clicks, conversions, conversions_value as conversion_value
	From "airup_eu_dwh"."adwords_custom_new_api"."ad_performance" ap
    LEFT JOIN country_account_mapping csam ON ap.customer_id in  (ltrim(rtrim(csam.adwords_customer_ids, '}'), '{'))
	--group by 1, 2, 3, 4
), facebook_ads as (
	select fap.date, fap.country_grouping, fap.country_fullname, 'Paid Social - Meta'::text AS channel, 
	fap.impressions, fap.cost, fap.click as clicks, fap.orders as conversions, fap.sales as conversion_value 
	from "airup_eu_dwh"."facebook_ads"."fct_daily_facebook_ads_performance" fap
	--group by 1, 2, 3, 4
), snapchat_ads as (
	select cdr."date"::date, am.country_fullname, am.country_grouping, 'Paid Social - Snapchat'::text as channel,
	cdr.impressions, cdr.spend / 1000000::float8 as cost, cdr.swipes as clicks, cdr.conversion_purchases as conversions, cdr.conversion_purchases_value / 1000000 as conversion_value 
	from "airup_eu_dwh"."snapchat_ads"."campaign_daily_report" cdr
	left join "airup_eu_dwh"."snapchat_ads"."campaign_history" ch 
	on ch.id = cdr.campaign_id 
	left join "airup_eu_dwh"."snapchat_ads"."fct_snap_account_mapping" am
	on am.id = ch.ad_account_id 
	--group by 1, 2, 3, 4
), tiktok_ads as (
	select acr.stat_time_day::date as date, 'Germany'::text as country_fullname, 'Central Europe':: text as country_grouping, 'Paid Social - TikTok'::text as channel,
	acr.impressions, acr.spend as cost, acr.clicks as clicks, acr.conversion as conversions, acr."result" as conversion_value
	from "airup_eu_dwh"."tiktok_ads"."ad_country_report" acr
	--group by 1, 2, 3, 4
), bing_ads as (
	select dbabc.date, dbabc.country_fullname, dbabc.country_grouping, 'SEA'::text as channel,
	dbabc.impressions, dbabc.cost, dbabc.clicks clicks, dbabc.conversions, conversion_value as conversion_value 
	from "airup_eu_dwh"."bingads"."fct_daily_bing_ads_by_campaigns" dbabc
	--group by 1, 2, 3, 4
),  combination AS (
	select date, country_fullname::varchar(256), country_grouping::varchar(256), channel::varchar(256), impressions::int, cost::float8, clicks::int, conversions::int, conversion_value::float8
	from adwords
	union
	select date, country_fullname::varchar(256), country_grouping::varchar(256), channel::varchar(256), impressions::int, cost::float8, clicks::int, conversions::int, conversion_value::float8
	from facebook_ads
	union
	select date, country_fullname::varchar(256), country_grouping::varchar(256), channel::varchar(256), impressions::int, cost::float8, clicks::int, conversions::int, conversion_value::float8
	from snapchat_ads
	union
	select date, country_fullname::varchar(256), country_grouping::varchar(256), channel::varchar(256), impressions::int, cost::float8, clicks::int, conversions::int, conversion_value::float8
	from tiktok_ads
	union
	select date, country_fullname::varchar(256), country_grouping::varchar(256), channel::varchar(256), impressions::int, cost::float8, clicks::int, conversions::int, conversion_value::float8
	from bing_ads	 
)
	select date, country_fullname, country_grouping, channel, sum(impressions) as impressions, sum(cost) as cost, sum(clicks) as clicks, sum(conversions) as conversions, sum(conversion_value) as conversion_value 
	from combination
	group by 1,2,3,4
  	order by date desc, channel, country_fullname