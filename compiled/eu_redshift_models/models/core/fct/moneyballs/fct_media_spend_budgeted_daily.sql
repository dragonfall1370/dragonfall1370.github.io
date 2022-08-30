

WITH date_series AS (
    WITH start_end AS (
        SELECT 
            '2020-01-01'::date AS start_date, 
            '2022-12-31'::date AS end_date,
            DATEDIFF(day, '2020-01-01', '2022-12-31') AS date_diff
    ), number_series AS (
        SELECT * FROM reports.series_of_number 
        WHERE gen_num BETWEEN 0 AND (SELECT date_diff FROM start_end )
    ) SELECT (end_date - gen_num) as "date" FROM start_end os
    JOIN number_series AS s ON 1 = 1
) SELECT 
    date_series."date" AS day_split,
    msb.region,
    msb.country,
        CASE
            WHEN msb.channel = 'Influencer' THEN 'influencer'
            WHEN msb.channel = 'Paid Social' THEN 'facebook'
            WHEN msb.channel = 'Youtube' OR msb.channel = 'Paid Search' OR msb.channel = 'Display' THEN 'google'
            ELSE 'other'
        END AS channel,
    SUM(COALESCE(msb.media_spend_budgeted, 0)) AS media_spend_budgeted,
    SUM(COALESCE(msb.media_spend_budgeted, 0)) / date_part(days, dateadd(day,-1,dateadd(month, 1, date_trunc('month', msb.month)))) AS media_spend_budget_per_day,
    date_part(days, dateadd(day,-1,dateadd(month, 1, date_trunc('month', msb.month))))  AS no_of_days_in_month
FROM "airup_eu_dwh"."reports"."seed_media_spend_budgeted" msb
JOIN  date_series
ON msb.month = date_trunc('month', date_series."date")
GROUP BY date_series."date", msb.region, msb.country, (
        CASE
            WHEN msb.channel = 'Influencer' THEN 'influencer'
            WHEN msb.channel = 'Paid Social' THEN 'facebook'
            WHEN msb.channel = 'Youtube' OR msb.channel = 'Paid Search' OR msb.channel = 'Display' THEN 'google'
            ELSE 'other'
        END), msb.month