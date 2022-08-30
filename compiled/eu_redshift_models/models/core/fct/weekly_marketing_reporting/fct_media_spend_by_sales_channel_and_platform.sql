

-- Since some influencer media spend cannot properly be assigned to either D2C or Amazon (see influencer.influencer_by_country), it is necessary tosplit this cost by another rule.
-- This is achieved by splitting the cost based on the share of orders for which each of the sales channels accounts.
with
d2c_order_share as
    (select
        shipping_country as country,
        country_grouping,
        "date",
        sum(case when sales_channel = 'd2c' then gross_orders::double precision end) / sum(gross_orders::double precision) as d2c_order_share
    from
        "airup_eu_dwh"."weekly_marketing_reporting"."fct_overall_revenue_order_value_daily"
    group by
        shipping_country,
        country_grouping,
        "date"),
        
-- union of media spend from the platforms google, facebook, influencer, and amazon
unionized_data as
    (
    -- #################################
    -- Google data
    -- adwords, smart-shopping, youtube
    -- #################################
    select
        'd2c'::text as sales_channel,
        'google'::text as media_platform,
        country_fullname as country,
        country_grouping,
        "date",
        sum("cost") as media_spend,
        case
            -- paid
            when
                ad_network_type = 'YOUTUBE_WATCH'
                or
                ad_network_type in ('MIXED', 'SEARCH_PARTNERS')
                or
                ad_network_type in ('CONTENT', 'SEARCH') 
                and 
                "name" ~~ '%s-generic%'
            then 'Paid'
            -- organic
            when
                ad_network_type in('CONTENT', 'SEARCH') 
                and "name" ~~ '%s-brand%'
            then 'Organic'
            -- other
            else 'Other'
        end as channel_grouping_lvl1,
        case
            when ad_network_type in ('YOUTUBE_WATCH', 'YOUTUBE_SEARCH') then 'Youtube'
            when ad_network_type in ('SEARCH','SEARCH_PARTNERS') then 'Search'
            when ad_network_type in ('MIXED') then 'Shopping'
            when ad_network_type in ('CONTENT') AND "name" ~~ '%a-video%' then 'Youtube'
            when ad_network_type in ('CONTENT') AND "name" ~~ '%a-display%' then 'Display'
            else 'Other'
        end as channel_grouping_lvl2
    from 
         "airup_eu_dwh"."adwords"."fct_custom_report_enriched"
       
        --adwords_custom.custom_report_enriched
    group by
        1, 2, 3, 4, 5, 7, 8
        
    union all
    
    
    -- #################################
    -- Facebook data
    -- fb, instagram, messenger
    -- #################################
    select
        'd2c'::text as sales_channel,
        'facebook'::text as media_platform,
        country_fullname as country,
        country_grouping,
        "date",
        sum(total_spend) as media_spend,
        'Paid'::text as channel_grouping_lvl1,
        'Social Paid'::text as channel_grouping_lvl2
    from
        "airup_eu_dwh"."facebook"."daily_facebook_ads_cost_enriched"
        --facebook.daily_facebook_ads_cost_enriched
    group by
        1, 2, 3, 4, 5, 7, 8
    
    
    union all
    
    
    -- #################################
    -- Influencer data
    -- fb, instagram, messenger
    -- #################################
    SELECT * FROM (
        SELECT 
            'd2c'::text AS sales_channel,
            'influencer'::text AS media_platform,
            influencer_enriched.country,
            influencer_enriched.region AS country_grouping,
            influencer_enriched."date",
            COALESCE(
                CASE
                    WHEN influencer_enriched.campaign IS NOT NULL AND influencer_enriched.campaign <> 'undefined' OR influencer_enriched.coupon_code IS NOT NULL THEN influencer_enriched.total_costs
                    ELSE NULL
                END, 0) + COALESCE(d2c_order_share.d2c_order_share *
                CASE
                    WHEN (influencer_enriched.campaign IS NULL OR influencer_enriched.campaign = 'undefined') AND influencer_enriched.coupon_code IS NULL THEN influencer_enriched.total_costs
                    ELSE NULL
                END, 0) AS media_spend,
            'Paid'::text as channel_grouping_lvl1,
            'Influencer'::text as channel_grouping_lvl2
        FROM "airup_eu_dwh"."influencer"."dim_influencer_enriched" influencer_enriched
        LEFT JOIN d2c_order_share ON d2c_order_share.country = influencer_enriched.country AND d2c_order_share.country_grouping = influencer_enriched.region AND d2c_order_share."date" = influencer_enriched."date"
        UNION ALL
        SELECT 
            'amz'::text AS sales_channel,
            'influencer'::text AS media_platform,
            influencer_enriched.country,
            influencer_enriched.region AS country_grouping,
            influencer_enriched."date",
            COALESCE((1 - d2c_order_share.d2c_order_share) *
                CASE
                    WHEN (influencer_enriched.campaign IS NULL OR influencer_enriched.campaign = 'undefined') AND influencer_enriched.coupon_code IS NULL THEN influencer_enriched.total_costs
                    ELSE NULL
                END, 0) AS media_spend,
            'Paid'::text as channel_grouping_lvl1,
            'Influencer'::text as channel_grouping_lvl2
        FROM "airup_eu_dwh"."influencer"."dim_influencer_enriched" influencer_enriched
        LEFT JOIN d2c_order_share ON d2c_order_share.country = influencer_enriched.country AND d2c_order_share.country_grouping = influencer_enriched.region AND d2c_order_share."date" = influencer_enriched."date"
    )
    union all
    
    -- #################################
    -- Amazon Advertising data
    -- #################################
    select
        'amz'::text AS sales_channel,
        'amazon'::text AS media_platform,
        country,
        country_grouping,
        date,
        amazon_media_spend AS media_spend,
        'Paid'::text AS channel_grouping_lvl1,
        'Amazon'::text AS channel_grouping_lvl2
    from "airup_eu_dwh"."amazon_advertising"."fct_amazon_media_spend"

        union all
    
    -- #################################
    -- TikTok data
    -- #################################
    select
        'tiktok'::text AS sales_channel,
        'tiktok'::text AS media_platform,
        country_fullname AS country,
        country_grouping,
        date,
        total_spend AS media_spend,
        'Paid'::text AS channel_grouping_lvl1,
        'Social Paid'::text AS channel_grouping_lvl2
    from "airup_eu_dwh"."tiktok_ads"."fct_daily_tiktok_ads_cost_by_country" daily_tiktok_ads_cost_by_country
    left join "airup_eu_dwh"."tiktok_ads"."fct_tiktok_account_mapping" tiktok_account_mapping ON daily_tiktok_ads_cost_by_country.region = tiktok_account_mapping.country_abbreviation 
    )
    
select
    sales_channel,
    media_platform,
    country,
    country_grouping,
    "date",
    sum(media_spend) as media_spend,
    channel_grouping_lvl1,
    channel_grouping_lvl2
from
    unionized_data
group by
    sales_channel,
    media_platform,
    channel_grouping_lvl1,
    channel_grouping_lvl2,
    country,
    country_grouping,
    "date",
    channel_grouping_lvl1,
    channel_grouping_lvl2