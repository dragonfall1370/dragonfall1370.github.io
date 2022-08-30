


select day, 
	case when shop = 'gmbh' then 'Germany'
		 when shop = 'fr' then 'France'
	 	 when shop = 'ch'then 'Switzerland'
		 when shop = 'uk' then 'United Kingdom'
		 when shop = 'nl' then 'Netherlands'
		 when shop = 'it' then 'Italy'
	end as country,
	ua_form_factor as device,
	sum(total_orders_placed) as total_orders_placed, 
	sum(total_carts) as total_carts,
	sum(total_checkouts) as total_checkouts,
	sum(total_sessions) as total_sessions 
from "airup_eu_dwh"."shopify_visits"."visits"
group by 1,2,3
order by 1 desc