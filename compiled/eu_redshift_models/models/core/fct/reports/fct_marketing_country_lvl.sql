-- Migrated by YuShih Hsieh
-- Last Modified by: YuShih Hsieh --> add SE



WITH google_analytics AS (
         SELECT bam.date,
                CASE
                    WHEN bam.profile::text = '224422635'::text THEN 'All Countries'::text
                    WHEN bam.profile::text = '191612850'::text THEN 'Germany'::text
                    WHEN bam.profile::text = '224409113'::text THEN 'United Kingdom'::text
                    WHEN bam.profile::text = '229262615'::text THEN 'Netherlands'::text
                    WHEN bam.profile::text = '224425388'::text THEN 'France'::text
                    WHEN bam.profile::text = '237974494'::text THEN 'Switzerland'::text
                    WHEN bam.profile::text = '248446647'::text THEN 'Italy'::text
                    WHEN bam.profile::text = '261933039'::text THEN 'Sweden'::text                        
                    ELSE 'view ID not mapped'::text
                END AS country,
            sum(bam.sessions) AS sessions,
            sum(bam.bounces) AS bounces,
            sum(bam.transactions) AS orders,
            sum(bam.pageviews) AS pageviews,
            sum(bam.transaction_revenue) AS revenue,
            sum(bam.transaction_tax) AS tax,
            sum(bam.transaction_shipping) AS shipping,
            sum(bam.transaction_revenue) - sum(bam.transaction_tax) - sum(bam.transaction_shipping) AS net_revenue,
            round(COALESCE(NULLIF(sum(bam.transactions), 0::numeric) / NULLIF(sum(bam.sessions), 0::numeric), 0::numeric), 4) AS conversion_rate
           FROM "airup_eu_dwh"."google_analytics"."basic_acquisition_metrics" bam
          GROUP BY bam.date, (
                CASE
                    WHEN bam.profile::text = '224422635'::text THEN 'All Countries'::text
                    WHEN bam.profile::text = '191612850'::text THEN 'Germany'::text
                    WHEN bam.profile::text = '224409113'::text THEN 'United Kingdom'::text
                    WHEN bam.profile::text = '229262615'::text THEN 'Netherlands'::text
                    WHEN bam.profile::text = '224425388'::text THEN 'France'::text
                    WHEN bam.profile::text = '237974494'::text THEN 'Switzerland'::text
                    WHEN bam.profile::text = '248446647'::text THEN 'Italy'::text
                    WHEN bam.profile::text = '261933039'::text THEN 'Sweden'::text                        
                    ELSE 'view ID not mapped'::text
                END)
          ORDER BY bam.date DESC
        )
 SELECT ga.date,
    ga.country,
    sum(ga.sessions) AS sessions,
    sum(ga.orders) AS orders,
    sum(ga.bounces) AS bounces,
    sum(ga.pageviews) AS pageviews,
    sum(ga.revenue) AS revenue,
    sum(ga.net_revenue) AS net_revenue,
    sum(ga.conversion_rate) AS conversion_rate
   FROM google_analytics ga
  GROUP BY ga.date, ga.country
  ORDER BY ga.date DESC