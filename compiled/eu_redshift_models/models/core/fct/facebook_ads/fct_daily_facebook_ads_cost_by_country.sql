

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
    TRIM(SPLIT_PART(ltrim(rtrim(B.facebook_account_ids , '}'), '{'), ',', NS.n))::int8 as "facebook_account_ids"
    from NS
    inner join "airup_eu_dwh"."public"."country_system_account_mapping" B 
    ON NS.n <= regexp_count(ltrim(rtrim(B.facebook_account_ids , '}'), '{'), ',') + 1
)
SELECT ccvr.date,
    csam.country_fullname,
    csam.country_grouping,
    csam.country_abbreviation AS region,
    'Paid Social'::text AS channel,
    'Meta'::text AS channel_subcategory,
    sum(ccvr.spend) AS total_spend
   FROM "airup_eu_dwh"."facebook_ads"."fct_custom_conversion_value_report_enriched" ccvr
     LEFT JOIN country_account_mapping csam ON ccvr.account_id in  (ltrim(rtrim(csam.facebook_account_ids, '}'), '{'))
     GROUP BY ccvr.date, csam.country_fullname, csam.country_grouping, csam.country_abbreviation
  ORDER BY ccvr.date DESC