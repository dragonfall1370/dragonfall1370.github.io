---created_by: Nham Dao
---###################################################################################################################
        -- this view contains the ticket information for kustomer schema (it cleans up the data from the original conversation table and remove the deleted tickets)
---###################################################################################################################


with conversation_enriched as (select conv.id,
	conv.created_at as created_at,
	conv.updated_at as updated_at,
	conv.message_count,
	conv.direction,
	conv.satisfaction_level_created_at as satisfaction_level_created_at,
	conv.last_activity_at as last_activity_at,
	conv.custom_sales_lead_bool,
	status,
	first_message_in_channel as source,
	reopen_count,
	INITCAP(replace(split_part(conv.default_lang , '_', 1),'_',' ')) as customer_language,
	INITCAP(replace(split_part(custom_contact_reason_tree, '.', 1),'_',' ')) as contact_reason,
	INITCAP(replace(split_part(custom_contact_reason_tree, '.', 2),'_',' ')) as sub_category1,
	INITCAP(replace(split_part(custom_contact_reason_tree, '.', 3),'_',' ')) as sub_category2, 
	INITCAP(replace(split_part(custom_contact_reason_tree, '.', 4),'_',' ')) as sub_category3, 
	satisfaction_level_rating, 
	first_response_created_at, 
    CASE 
		WHEN cust.custom_country_tree LIKE '%austria%' THEN 'Austria'
		WHEN cust.custom_country_tree LIKE '%belgium%' THEN 'Belgium'
		WHEN cust.custom_country_tree LIKE '%france%' THEN 'France'
		WHEN cust.custom_country_tree LIKE '%germany%' THEN 'Germany'
		WHEN cust.custom_country_tree LIKE '%italy%' THEN 'Italy'
		WHEN cust.custom_country_tree LIKE '%netherlands%' THEN 'Netherlands'
		WHEN cust.custom_country_tree LIKE '%sweden%' THEN 'Sweden'
		WHEN cust.custom_country_tree LIKE '%switzerland%' THEN 'Switzerland'
		WHEN cust.custom_country_tree LIKE '%england%' THEN 'United Kingdom'
		WHEN cust.custom_country_tree LIKE '%usa%' THEN 'United States'
		ELSE 'other'
	END	as customer_country
from
	"airup_eu_dwh"."kustomer"."conversation" conv
left join "airup_eu_dwh"."kustomer"."customer" cust 
on conv.customer_id = cust.id
where conv.deleted is null
and first_message_in_channel is not null)
select conversation_enriched.*, 
case when country_mapping.country_grouping is not null then country_mapping.country_grouping
else 'Unspecified' end as country_grouping from conversation_enriched
left join "airup_eu_dwh"."public"."country_system_account_mapping" country_mapping 
on country_mapping.country_fullname = conversation_enriched.customer_country