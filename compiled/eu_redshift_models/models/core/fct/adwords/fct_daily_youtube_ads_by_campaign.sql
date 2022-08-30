

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
)
SELECT cr.date,
    cr.customer_id,
    csam.country_grouping,
    csam.country_fullname,
    csam.country_abbreviation as region,
    cr.campaign_id,
    cr.campaign_name,
        CASE
            WHEN cr.campaign_name::text ~~ '%_s-retargeting%'::text THEN 'Retargeting'::text
            WHEN cr.campaign_name::text ~~ '%_s-retention%'::text THEN 'Customer'::text
            WHEN cr.campaign_name::text ~~ '%_s-prospecting%'::text THEN 'Prospecting'::text
            ELSE 'Other'::text
        END AS campaign_type,
    cr. title AS video_title,
    cr.video_views,
    sum(cr.impressions) AS impressions,
    sum(cr.clicks) AS clicks,
    sum(cr.cost_micros) /1000000::float8 AS cost,
    sum(cr.conversions) AS conversions,
    sum(cr.conversions_value) AS conversion_value
   FROM "airup_eu_dwh"."adwords_new_api"."video_performance_report" cr
     LEFT JOIN country_account_mapping csam ON cr.customer_id in  (ltrim(rtrim(csam.adwords_customer_ids, '}'), '{'))
  GROUP BY 1,2,3,4,5,6,7,8,9,10
  ORDER BY cr.date DESC