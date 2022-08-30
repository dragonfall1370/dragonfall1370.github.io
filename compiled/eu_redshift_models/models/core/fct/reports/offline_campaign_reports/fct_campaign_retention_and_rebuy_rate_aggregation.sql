

select
    a.cohort,
    a.country,
    --a.date_diff_weeks,
    a.date_diff_months,
    a.cohort_size,
    a.month,
    a.customers,
    a.retention_rate,
    a.orders,
    a.rebuy_rate,
    b.retention_rate as non_campaign_retention_rate,
    b.rebuy_rate as non_campaign_rebuy_rate
from
    "airup_eu_dwh"."reports"."fct_campaign_retention_and_rebuy_rate_offline" a
    --dbt_feldm.dim_offline_campaign_retention_and_rebuy_rate a
        left join "airup_eu_dwh"."reports"."fct_campaign_retention_and_rebuy_rate_non_offline" b
        --left join dbt_feldm.dim_non_offline_campaign_retention_and_rebuy_rate b
            on a.cohort = b.cohort
            and a.country = b.country
            --and a.date_diff_weeks = b.date_diff_weeks
            and a.date_diff_months = b.date_diff_months
            and a.month = b.month