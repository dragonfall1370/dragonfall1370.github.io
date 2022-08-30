---owner: YuShih
---last modified: Nham Dao: use "case when.." to replace '' to null so that we can convert budget text to number later




WITH ad_budget as (
         SELECT ad_budget.date::date, 
               ad_budget.region, 
               case when ad_budget.display <> '' then ad_budget.display else null end as display,
               case when ad_budget.tiktok <> '' then ad_budget.tiktok else null end as tiktok,
               case when ad_budget.snapchat <> '' then ad_budget.snapchat else null end as snapchat,
               case when ad_budget.paid_social <> '' then ad_budget.paid_social else null end as paid_social,
               case when ad_budget.video <> '' then ad_budget.video else null end as video,
               case when ad_budget.sea <> '' then ad_budget.sea else null end as sea
             FROM "airup_eu_dwh"."ads_budget_tracker"."ad_budget" ad_budget 
),
 display AS (
         SELECT ad_budget.date::date,
            ad_budget.region,
            'Display'::text AS channel,
            'Display'::text AS channel_subcategory,
            ad_budget.display AS total_spend
           FROM ad_budget
        ), meta AS (
         SELECT ad_budget.date::date,
            ad_budget.region,
            'Paid Social'::text AS channel,
            'Meta'::text AS channel_subcategory,
            ad_budget.paid_social AS total_spend
           FROM ad_budget
        ), tiktok AS (
         SELECT ad_budget.date::date,
            ad_budget.region,
            'Paid Social'::text AS channel,
            'TikTok'::text AS channel_subcategory,
            ad_budget.tiktok AS total_spend
           FROM ad_budget   
        ), snapchat AS (
         SELECT ad_budget.date::date,
            ad_budget.region,
            'Paid Social'::text AS channel,
            'Snapchat'::text AS channel_subcategory,
            ad_budget.snapchat AS total_spend
           FROM ad_budget
        ), video AS (
         SELECT ad_budget.date::date,
            ad_budget.region,
            'Video'::text AS channel,
            'Video'::text AS channel_subcategory,
            ad_budget.video AS total_spend
           FROM ad_budget
        ), sea AS (
         SELECT ad_budget.date::date,
            ad_budget.region,
            'SEA'::text AS channel,
            'SEA'::text AS channel_subcategory,
            ad_budget.sea AS total_spend
           FROM ad_budget
        ), combination AS (
         SELECT display.date,
            display.region,
            display.channel,
            display.channel_subcategory,
            display.total_spend::double precision as total_spend
           FROM display
        UNION
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
         SELECT snapchat.date,
            snapchat.region,
            snapchat.channel,
            snapchat.channel_subcategory,
            snapchat.total_spend::double precision as total_spend
           FROM snapchat                                 
        UNION
         SELECT video.date,
            video.region,
            video.channel,
            video.channel_subcategory,
            video.total_spend::double precision as total_spend
           FROM video
        UNION
         SELECT sea.date,
            sea.region,
            sea.channel,
            sea.channel_subcategory,
            sea.total_spend::double precision as total_spend
           FROM sea
        )
 SELECT combination.date,
    combination.region,
    combination.channel,
    combination.channel_subcategory,
    SUM(combination.total_spend) as total_spend
   FROM combination
  GROUP BY 1,2,3,4
  ORDER BY 1 desc,2,3,4