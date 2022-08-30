

SELECT to_char(date_trunc('month', date), 'yyyyMM') AS year_month,
    to_char(asp.date, 'yyyyMMdd') AS date,
    date_trunc('month', date) = date_trunc('month', CURRENT_DATE) AS current_month,
    date_trunc('month', date) = (date_trunc('month', CURRENT_DATE) - '1 mon'::interval) AS previous_month,
    date_trunc('quarter', date) = date_trunc('quarter', CURRENT_DATE) AS current_quarter,
    sum(asp.cost) AS ga_ad_cost,
        CASE
            WHEN date_trunc('month', date) = (date_trunc('month', CURRENT_DATE) - '1 mon'::interval) 
            AND date_part('day', date) <= (date_part('day', CURRENT_DATE) - 1::double precision) THEN 'MTD'
            ELSE NULL
        END AS mtd_qualifier
   FROM "airup_eu_dwh"."adwords"."account_stats_prebuilt" asp
  GROUP BY date
  ORDER BY (to_char(date, 'yyyyMMdd'))