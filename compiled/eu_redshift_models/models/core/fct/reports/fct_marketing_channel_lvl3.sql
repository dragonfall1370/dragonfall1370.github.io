-- changed source for adwords_custom from fct to raw table (Abhishek Pathak, 10-Mar-2022)
-- changed source for google_analytics from fct to raw table (YuShih Hsieh, 30-Mar-2022)

 

 WITH google AS (
         SELECT cr.date,
                CASE
                    WHEN cr.ad_network_type_2 = 'YouTube Videos' THEN 'Youtube'
                    WHEN cr.ad_network_type_2 IN ('Cross-network', 'Search partners') THEN 'Smart Shopping'
                    WHEN cr.ad_network_type_2 IN ('Display Network', 'Google search') THEN 'Search'
                    ELSE 'Other'
                END AS channel_grouping_lvl2,
            round(sum(cr.cost), 0) AS costs
           FROM "airup_eu_dwh"."adwords_custom"."custom_report" cr
          GROUP BY cr.date, (
                CASE
                    WHEN cr.ad_network_type_2 = 'YouTube Videos' THEN 'Youtube'
                    WHEN cr.ad_network_type_2 IN ('Cross-network', 'Search partners') THEN 'Smart Shopping'
                    WHEN cr.ad_network_type_2 IN ('Display Network', 'Google search') THEN 'Search'
                    ELSE 'Other'
                END)
          ORDER BY cr.date DESC
        ), facebook AS (
         SELECT dfac.date,
            'Social Paid'::text AS channel_grouping_lvl2,
            round(sum(dfac.total_spend), 2) AS costs
           FROM "airup_eu_dwh"."facebook"."daily_facebook_ads_cost" dfac
          GROUP BY dfac.date, 'Social Paid'::text
          ORDER BY dfac.date DESC
        ), influencer AS (
         SELECT cmuttde.date,
            'Influencer'::text AS channel_grouping_lvl2,
            round(sum(cmuttde.total_costs), 2) AS costs
           FROM "airup_eu_dwh"."influencer"."dim_influencer_enriched" cmuttde
          GROUP BY cmuttde.date, 'Influencer'::text
  ---ORDER BY 1 DESC
        ), google_analytics AS (
         SELECT bam.date,
                CASE
                    WHEN bam.profile = '224422635' THEN 'All Countries'
                    WHEN bam.profile = '191612850' THEN 'Germany'
                    WHEN bam.profile = '224409113' THEN 'United Kingdom'
                    WHEN bam.profile = '229262615' THEN 'Netherlands'
                    WHEN bam.profile = '224425388' THEN 'France'
                    ELSE 'view ID not mapped'
                END AS country,
                CASE
                    WHEN bam.channel_grouping = 'Social Paid' THEN 'Social Paid'
                    WHEN bam.channel_grouping = 'Social Organic' THEN 'Social Organic'
                    WHEN bam.channel_grouping = 'Organic Search' THEN 'Organic Search'
                    WHEN bam.channel_grouping IN ('Paid Search Branded', 'Paid Search Generic', 'Smart Shopping') THEN 'Search'
                    WHEN bam.channel_grouping = 'Email' THEN 'Email'
                    WHEN bam.channel_grouping = 'Influencer' THEN 'Influencer'
                    WHEN bam.channel_grouping = 'Referral' THEN 'Referral + PR'
                    WHEN bam.channel_grouping = 'Video Paid' THEN 'Youtube'
                    WHEN bam.channel_grouping = 'Direct' THEN 'Direct'
                    ELSE 'Other'
                END AS channel_grouping_lvl2,
            sum(bam.sessions) AS sessions,
            sum(bam.bounces) AS bounces,
            sum(bam.transactions) AS orders,
            sum(bam.transaction_revenue) AS revenue,
            sum(bam.transaction_tax) AS tax,
            sum(bam.transaction_shipping) AS shipping,
            sum(bam.transaction_revenue) - sum(bam.transaction_tax) - sum(bam.transaction_shipping) AS net_revenue,
            round(COALESCE(NULLIF(sum(bam.transactions), 0) / NULLIF(sum(bam.sessions), 0), 0), 4) AS conversion_rate
           FROM "airup_eu_dwh"."google_analytics"."basic_acquisition_metrics" bam

          GROUP BY bam.date, (
                CASE
                    WHEN bam.profile = '224422635' THEN 'All Countries'
                    WHEN bam.profile = '191612850' THEN 'Germany'
                    WHEN bam.profile = '224409113' THEN 'United Kingdom'
                    WHEN bam.profile = '229262615' THEN 'Netherlands'
                    WHEN bam.profile = '224425388' THEN 'France'
                    ELSE 'view ID not mapped'
                END), (
                CASE
                    WHEN bam.channel_grouping = 'Social Paid' THEN 'Social Paid'
                    WHEN bam.channel_grouping = 'Social Organic' THEN 'Social Organic'
                    WHEN bam.channel_grouping = 'Organic Search' THEN 'Organic Search'
                    WHEN bam.channel_grouping IN ('Paid Search Branded', 'Paid Search Generic', 'Smart Shopping') THEN 'Search'
                    WHEN bam.channel_grouping = 'Email' THEN 'Email'
                    WHEN bam.channel_grouping = 'Influencer' THEN 'Influencer'
                    WHEN bam.channel_grouping = 'Referral' THEN 'Referral + PR'
                    WHEN bam.channel_grouping = 'Video Paid' THEN 'Youtube'
                    WHEN bam.channel_grouping = 'Direct' THEN 'Direct'
                    ELSE 'Other'
                END)
          ORDER BY bam.date DESC
        )
 SELECT ga.date,
    ga.country,
    ga.channel_grouping_lvl2,
    sum(
        CASE
            WHEN ga.country = 'All Countries' THEN g.costs
            ELSE NULL
        END) AS costs_google,
    sum(
        CASE
            WHEN ga.country = 'All Countries' THEN fb.costs
            ELSE NULL
        END) AS costs_facebook,
    sum(
        CASE
            WHEN ga.country = 'All Countries' THEN i.costs
            ELSE NULL
        END) AS costs_influencer,
    sum(COALESCE(
        CASE
            WHEN ga.country = 'All Countries' THEN g.costs
            ELSE NULL
        END, 0)) + sum(COALESCE(
        CASE
            WHEN ga.country = 'All Countries' THEN fb.costs
            ELSE NULL
        END, 0)) + sum(COALESCE(
        CASE
            WHEN ga.country = 'All Countries' THEN i.costs
            ELSE NULL
        END, 0)) AS media_costs,
    sum(ga.sessions) AS sessions,
    sum(ga.bounces) AS bounces,
    sum(ga.orders) AS orders,
    sum(ga.revenue) AS revenue,
    sum(ga.net_revenue) AS net_revenue,
    sum(ga.conversion_rate) AS conversion_rate,
    round(COALESCE(NULLIF(sum(
        CASE
            WHEN ga.country = 'All Countries' THEN fb.costs
            ELSE NULL
        END), 0) / NULLIF(sum(ga.revenue), 0), 0), 4) AS cpr_facebook,
    round(COALESCE(NULLIF(sum(
        CASE
            WHEN ga.country = 'All Countries' THEN g.costs
            ELSE NULL
        END), 0) / NULLIF(sum(ga.revenue), 0), 0), 4) AS cpr_google,
    round(COALESCE(NULLIF(sum(
        CASE
            WHEN ga.country = 'All Countries' THEN i.costs
            ELSE NULL
        END), 0) / NULLIF(sum(ga.revenue), 0), 0), 4) AS cpr_influencer
   FROM google_analytics ga
     LEFT JOIN google g USING (channel_grouping_lvl2, date)
     LEFT JOIN facebook fb USING (channel_grouping_lvl2, date)
     LEFT JOIN influencer i USING (channel_grouping_lvl2, date)
  GROUP BY ga.date, ga.country, ga.channel_grouping_lvl2
  ORDER BY ga.date DESC