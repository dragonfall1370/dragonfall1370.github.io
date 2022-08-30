---Authors: YuShih Hsieh
---Last Modified by: YuShih Hsieh

---###################################################################################################################
        -- this view contains the information on the Google Analytics landingpage metrics for E-commerce dashboard
---###################################################################################################################




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
    TRIM(SPLIT_PART(ltrim(rtrim(B.google_analytics_profiles , '}'), '{'), ',', NS.n))::int8 as "google_analytics_profiles"
    from NS
    inner join "airup_eu_dwh"."public"."country_system_account_mapping" B 
    ON NS.n <= regexp_count(ltrim(rtrim(B.google_analytics_profiles , '}'), '{'), ',') + 1
), currency as (
    select csam.country_fullname,    
    case when csam.country_fullname = 'Austria' then 'EUR'
        when csam.country_fullname = 'Germany' then 'EUR'
        when csam.country_fullname = 'Switzerland' then 'CHF'
        when csam.country_fullname = 'France' then 'EUR'
        when csam.country_fullname = 'Italy' then 'EUR'
        when csam.country_fullname = 'Sweden' then 'SEK'
        when csam.country_fullname = 'Belgium' then 'EUR'
        when csam.country_fullname = 'Netherlands' then 'EUR'
        when csam.country_fullname = 'United Kingdom' then 'GBP'
        when csam.country_fullname = 'United States' then 'USD'
    end as currency 
    from country_account_mapping csam
), global_curr_usd as (
	SELECT 
		date(global_currency_rates.creation_datetime) AS creation_date,
    	global_currency_rates.currency_abbreviation,
    	global_currency_rates.conversion_rate_eur
    FROM "airup_eu_dwh"."odoo_currency"."dim_global_currency_rates" global_currency_rates
    WHERE global_currency_rates.currency_abbreviation::text = 'USD'::text
)
SELECT pp.date,
    pp.profile,
    csam.country_fullname,
    csam.country_abbreviation AS shop,
        CASE
            WHEN pp.user_type = true THEN 'New User'::character varying
            ELSE 'Returning User'::character varying
        END AS user_type,
    pp.segment,
    pp.device_category,
        CASE
            WHEN pp.landing_page_path::text ~~ '%checkouts%'::text THEN "left"(pp.landing_page_path::text, 22)::character varying
            WHEN pp.landing_page_path::text ~~ '/account/orders/%'::text THEN "left"(pp.landing_page_path::text, 15)::character varying
            WHEN pp.landing_page_path::text ~~ '%orders%'::text THEN "left"(pp.landing_page_path::text, 19)::character varying
            ELSE pp.landing_page_path
        END AS landing_page_path,
    currency.currency,
    dgcr.conversion_rate_eur,    
    sum(pp.pageviews) AS pageviews,
    sum(pp.unique_pageviews) AS unique_pageviews,
    sum(pp.time_on_page) AS time_on_page,
    sum(pp.exits) AS exits,
    sum(pp.bounces) AS bounces,
    sum(pp.sessions) AS sessions,
    sum(pp.goal_completions_all) AS goal_completions_all,
    sum(pp.transactions) as transactions,
    sum(pp.transaction_revenue) as revenue_original_currency,
    -- convert revenue from different currencies to EUR 
    SUM(pp.transaction_revenue / dgcr.conversion_rate_eur) as revenue,
    -- convert revenue from EUR to USD
    case when shop = 'US' then SUM(pp. transaction_revenue)
        else SUM(pp.transaction_revenue / dgcr.conversion_rate_eur * global_curr_usd.conversion_rate_eur) 
    end as revenue_usd
   FROM "airup_eu_dwh"."google_analytics"."landingpage_performance" pp
     LEFT JOIN country_account_mapping csam ON pp.profile in  (ltrim(rtrim(csam.google_analytics_profiles, '}'), '{'))
     left join currency on currency.country_fullname = csam.country_fullname
     Left join "airup_eu_dwh"."odoo_currency"."dim_global_currency_rates" dgcr on currency.currency = dgcr.currency_abbreviation and pp.date = dgcr.creation_date
     left join global_curr_usd on pp."date" = global_curr_usd.creation_date
  WHERE pp.profile::text <> '224422635'::text AND pp.date >= '2022-01-01'::date AND pp.landing_page_path::text ~~ '/pages/%'::text
  GROUP BY 1,2,3,4,5,6,7,8,9,10
  ORDER BY pp.date DESC