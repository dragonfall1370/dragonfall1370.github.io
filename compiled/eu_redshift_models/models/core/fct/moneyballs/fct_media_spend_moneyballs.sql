

WITH global_curr_usd AS (
	SELECT 
		date(global_currency_rates.creation_date) AS creation_date,
    	global_currency_rates.currency_abbreviation,
    	global_currency_rates.conversion_rate_eur
    FROM "airup_eu_dwh"."odoo_currency"."dim_global_currency_rates" global_currency_rates
    WHERE global_currency_rates.currency_abbreviation::text = 'USD'::text
) select 
    fct_media_spend_by_sales_channel_and_platform.date,
    fct_media_spend_by_sales_channel_and_platform.country,
    fct_media_spend_by_sales_channel_and_platform.country_grouping AS region,
    fct_media_spend_by_sales_channel_and_platform.media_platform,
    sum(fct_media_spend_by_sales_channel_and_platform.media_spend) AS media_spend,
    sum(fct_media_spend_by_sales_channel_and_platform.media_spend)*coalesce(global_curr_usd.conversion_rate_eur::double precision, 1.0801) AS media_spend_usd
from "airup_eu_dwh"."weekly_marketing_reporting"."fct_media_spend_by_sales_channel_and_platform" fct_media_spend_by_sales_channel_and_platform
LEFT JOIN global_curr_usd ON "date" = global_curr_usd.creation_date
group by fct_media_spend_by_sales_channel_and_platform.date, fct_media_spend_by_sales_channel_and_platform.country, fct_media_spend_by_sales_channel_and_platform.country_grouping, fct_media_spend_by_sales_channel_and_platform.media_platform, global_curr_usd.conversion_rate_eur