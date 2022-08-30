

WITH meta AS (
         SELECT fdfacbc.date::date,
            fdfacbc.region,
            fdfacbc.channel,
            fdfacbc.channel_subcategory,
            fdfacbc.total_spend
           FROM "airup_eu_dwh"."facebook_ads"."fct_daily_facebook_ads_cost_by_country" fdfacbc
        ), tiktok AS (
         SELECT fdtacbc.date::date,
            fdtacbc.region,
            fdtacbc.channel,
            fdtacbc.channel_subcategory,
            fdtacbc.total_spend
           FROM "airup_eu_dwh"."tiktok_ads"."fct_daily_tiktok_ads_cost_by_country" fdtacbc   
        ), snapchat AS (
         SELECT fdsacbc.date::date,
            fdsacbc.region,
            fdsacbc.channel,
            fdsacbc.channel_subcategory,
            fdsacbc.total_spend
           FROM "airup_eu_dwh"."snapchat_ads"."fct_daily_snap_ads_cost_by_country" fdsacbc
        ), adwords AS (
         SELECT fdgacbc.date::date,
            fdgacbc.region,
            fdgacbc.channel,
            fdgacbc.channel_subcategory,
            fdgacbc.total_spend
           FROM "airup_eu_dwh"."adwords"."fct_daily_google_ads_cost_by_country" fdgacbc
        ), bing AS (
         SELECT fdbacbc.date::date,
            fdbacbc.region,
            fdbacbc.channel,
            fdbacbc.channel_subcategory,
            fdbacbc.total_spend
           FROM "airup_eu_dwh"."bingads"."fct_daily_bing_ads_cost_by_country" fdbacbc               
        ), combination AS (
         SELECT meta.date,
            meta.region,
            meta.channel,
            meta.channel_subcategory,
            meta.total_spend::double precision as total_spend
           FROM meta
        UNION
         SELECT tiktok.date,
            tiktok.region,
            tiktok.channel,
            tiktok.channel_subcategory,
            tiktok.total_spend::double precision as total_spend
           FROM tiktok           
        UNION
         SELECT adwords.date,
            adwords.region,
            adwords.channel,
            adwords.channel_subcategory,
            adwords.total_spend::double precision as total_spend
           FROM adwords
        UNION
         SELECT bing.date,
            bing.region,
            bing.channel,
            bing.channel_subcategory,
            bing.total_spend::double precision as total_spend
           FROM bing
        )
         SELECT combination.date,
        combination.region,
        combination.channel,
        combination.channel_subcategory,
        SUM(combination.total_spend) as total_spend
    FROM combination
    GROUP BY 1,2,3,4
    ORDER BY 1 desc,2,3,4