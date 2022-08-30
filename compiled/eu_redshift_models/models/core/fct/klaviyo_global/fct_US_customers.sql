 

SELECT 
	email,
	p.id,
	first_name,
	last_name,
	address_1,
	address_2,
	city,
	country,
	zip,
	custom_source,
	custom_consent,
	custom_first_active,
	custom_last_active,
	custom_initial_source,
	SUM(CASE WHEN "type" = 'Placed Order' THEN 1 ELSE 0 END) AS order_count,
	SUM(e.property_value) AS total_revenue,
	custom_first_purchase_date
FROM "airup_eu_dwh"."klaviyo_global"."dim_event" e 
LEFT JOIN "airup_eu_dwh"."klaviyo_global"."dim_person" p ON e.person_id = p.id 
WHERE country = 'United States'
GROUP BY 	email,
	p.id,
	first_name,
	last_name,
	address_1,
	address_2,
	city,
	country,
	zip,
	custom_source,
	custom_consent,
	custom_first_active,
	custom_last_active,
	custom_initial_source,
	custom_first_purchase_date