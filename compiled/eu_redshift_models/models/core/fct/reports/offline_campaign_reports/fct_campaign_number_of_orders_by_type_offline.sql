

WITH
	campaign_orders_by_type as (
		select 
			id as order_id,	
			created_at,
			billing_address_country_code,
			case when campaign_with_code and not campaign_with_utm then 1 else 0 end as order_with_only_campaign_code,
			case when campaign_with_utm and not campaign_with_code then 1 else 0 end as order_with_only_campaign_utm,
			case when campaign_with_code and campaign_with_utm then 1 else 0 end as order_with_both_campaign_code_and_utm
		from 
			--dbt_feldm.fct_order_enriched_campaign_enriched_offline
            "airup_eu_dwh"."reports"."fct_order_enriched_campaign_enriched_offline" 
		where 
			campaign_purchase
	)
	
-- ###################
-- main query
-- ####################
	
	select
		date_trunc('day', created_at) as "date",
		billing_address_country_code as country,
		sum(order_with_only_campaign_code) as orders_with_only_campaign_code,
		sum(order_with_only_campaign_utm) as orders_with_only_campaign_utm,
		sum(order_with_both_campaign_code_and_utm) as orders_with_both_campaign_code_and_utm
	from 
		campaign_orders_by_type
	group by
		date_trunc('day', created_at),
		billing_address_country_code