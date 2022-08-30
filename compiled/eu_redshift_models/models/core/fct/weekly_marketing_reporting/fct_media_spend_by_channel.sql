

SELECT marketing_spend.reported_month,
    marketing_spend.region,
    marketing_spend.account_name,
    sum(marketing_spend.amount) AS media_spend
   FROM "airup_eu_dwh"."weekly_marketing_reporting"."marketing_spend" marketing_spend
  GROUP BY marketing_spend.reported_month, marketing_spend.region, marketing_spend.account_name