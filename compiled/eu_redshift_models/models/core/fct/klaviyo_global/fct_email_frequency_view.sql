


SELECT 
	date_trunc('month',send_time)::date AS "month"
	, SUM(successful_deliveries) AS emails
	, SUM(unsubscribes) AS unsubscribes
	, SUM(customers) AS customers
--	, SUM(revenue) AS revenue
	, shop
	, custom_consent
	,'campaign' AS email_type
FROM "airup_eu_dwh"."klaviyo_global"."fct_campaign_view_consent"
GROUP BY date_trunc('month',send_time)::date, shop
 , custom_consent

UNION ALL

SELECT
	date_trunc('month',send_time)::date AS "month" 
	, SUM(successful_deliveries) AS emails
	, SUM(unsubscribes) AS unsubscribes
	, SUM(customers) AS customers
--	, SUM(revenue) AS revenue
	, shop
	, custom_consent
	,'flow' AS email_type
FROM "airup_eu_dwh"."klaviyo_global"."fct_flow_view_consent"
GROUP BY date_trunc('month',send_time)::date, shop
 , custom_consent