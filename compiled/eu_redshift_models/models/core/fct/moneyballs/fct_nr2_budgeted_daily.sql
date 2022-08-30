

WITH most_recent_data AS (
SELECT * FROM "airup_eu_dwh"."living_budget"."finance_living_budget" 
-- WHERE date_part(year, "month") = 2022 
WHERE budgeted_day = ( SELECT MAX(budgeted_day) FROM "airup_eu_dwh"."living_budget"."finance_living_budget" WHERE date_part(year, "month") = 2022 )
UNION ALL
SELECT * FROM "airup_eu_dwh"."living_budget"."finance_living_budget"
WHERE budgeted_day = ( SELECT MAX(budgeted_day) FROM "airup_eu_dwh"."living_budget"."finance_living_budget" WHERE date_part(year, "month") = 2021 ) 
), date_series AS (
    WITH start_end AS (
        SELECT 
            '2020-01-01'::date AS start_date, 
            '2022-12-31'::date AS end_date,
            DATEDIFF(day, '2020-01-01', '2022-12-31') AS date_diff
    ), number_series AS (
        SELECT * FROM reports.series_of_number 
        WHERE gen_num BETWEEN 0 AND (SELECT date_diff FROM start_end)
    ) SELECT (end_date - gen_num) as "date" FROM start_end os
    JOIN number_series AS s ON 1 = 1
) SELECT 
    date_series."date" AS day_split,
    most_recent_data.region,
    most_recent_data.country,
    most_recent_data.channel,
    most_recent_data.nr_2_budgeted,
    most_recent_data.nr_2_budgeted / date_part(days, dateadd(day,-1,dateadd(month, 1, date_trunc('month', most_recent_data.month)))) AS nr2_buget_per_day,
    date_part(days, dateadd(day,-1,dateadd(month, 1, date_trunc('month', most_recent_data.month)))) AS no_of_days_in_month
   FROM date_series
    JOIN most_recent_data ON most_recent_data.month = date_trunc('month', date_series."date")