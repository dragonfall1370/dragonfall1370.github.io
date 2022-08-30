

with klaviyo_dimensions as
--this etc contains information of customers who joined nps1 and nps2
  (
select
	lower(person.email) as email,
	person.id as customer_id,
	person.custom_nps_1,
	person.custom_nps_2,
	person.country,
	CURRENT_DATE - person.custom_first_purchase_date as customer_duration_days,
	person.custom_first_purchase_date as first_purchase_date, 
	person.shop as NPS_shop, 
	case when custom_consent = '["email"]' then 1 else 0 end as opt_to_email
from
	"airup_eu_dwh"."klaviyo_global"."dim_person" person
where
	(custom_nps_1 is not null
		or custom_nps_2 is not null)
	and first_purchase_date is not null),
     product_category as
--this etc classifies different starter set products and flavours
  (
select
	subcategory_3,
	sku,
	category,
	case
		when subcategory_1 = 'Starter Set' and ((subcategory_3 like '%Charcoal%')
			or (subcategory_3 like '%White%')) then 'Starter Set'
			when subcategory_1 = 'Starter Set' and ((subcategory_3 like '%Electric Orange%')
				or (subcategory_3 like '%Hot Pink%')
					or (subcategory_3 like '%Ocean Blue%')) then 'Colored Bottle'
					when subcategory_1 = 'Starter Set' and (subcategory_3 like '%Bundle%') then 'Bundle'
					when (subcategory_3 like '%Wildberry%') then 'Wildberry'
					when (subcategory_3 not like '%Wildberry%')
						and category = 'Flavour' then 'Other Flavour'
						else 'Unspecified'
					end as bottle_flavour_type
				from
					"airup_eu_dwh"."shopify_global"."shopify_product_categorisation"),
     shopify_dimensions as
--this etc contains the orders of customers who join nps
  (
select
	lower("order".email) as email,
	"order".id,
	case
		when "order".shipping_address_country_code in ('DE',
                                                             'AT')
			and created_at >= '2021-07-27'
			and "order".shopify_shop = 'Base'
			and "order".order_number !~~ '%-%' then CONCAT('DE-', "order".order_number)
			else "order".order_number
		end as order_number,
		"order".created_at::date,
		"order".total_price,
		LISTAGG (order_line.title,
		', ') as basket,
		count(*) as number_of_product,
		case
			when SUM(case
                           when category = 'Hardware' then 1
                           else 0
                       end) >= 1 then 1
			else 0
		end as order_incl_bottle,
		case
			when SUM(case
                           when bottle_flavour_type = 'Starter Set' then 1
                           else 0
                       end) >= 1 then 1
			else 0
		end as order_incl_starter_set,
		case
			when SUM(case
                           when bottle_flavour_type = 'Colored Bottle' then 1
                           else 0
                       end) >= 1 then 1
			else 0
		end as order_incl_colored_bottle,
		case
			when SUM(case
                           when bottle_flavour_type = 'Bundle' then 1
                           else 0
                       end) >= 1 then 1
			else 0
		end as order_incl_bundle,
		case
			when SUM(case
                           when bottle_flavour_type = 'Wildberry' then 1
                           else 0
                       end) >= 1 then 1
			else 0
		end as order_incl_wildberry,
		case
			when SUM(case
                           when bottle_flavour_type = 'Other Flavour' then 1
                           else 0
                       end) >= 1 then 1
			else 0
		end as order_incl_other_flavour,
		sum(case
                  when category = 'Hardware' then 1
                  else 0
              end) as number_of_bottle
	from
		"airup_eu_dwh"."shopify_global"."fct_order_line" order_line
	left join "airup_eu_dwh"."shopify_global"."fct_order_enriched_ngdpr" "order" on
		order_line.order_id = "order".id
	left join product_category on
		product_category.sku = order_line.sku
	where
		lower("order".email) in
		--use lower(email) since customers might enter upper/lower case in different orders
       (
		select
			email
		from
			klaviyo_dimensions)
	group by
		lower("order".email),
		"order".id,
		"order".order_number,
		"order".shipping_address_country_code,
		"order".shopify_shop,
		"order".created_at,
		"order".total_price),
	 referer_customer as (
	select person_id, 
	case when sum(case when type in ('Registered as Referrer from Mention Me', 'Registered as Referee from Mention Me') then 1 else 0 end)>1 
	then 1 else 0 end as referral
	from "airup_eu_dwh"."klaviyo_global"."dim_event"
	group by 1),
     all_order_from_klaviyo_customer as
---this cte combine klaviyo and shopify orders of customers who join nps surveys
  (
select
	klaviyo_dimensions.customer_id,
	klaviyo_dimensions.custom_nps_1,
	klaviyo_dimensions.custom_nps_2,
	klaviyo_dimensions.country,
	klaviyo_dimensions.customer_duration_days,
	klaviyo_dimensions.first_purchase_date,
	klaviyo_dimensions.NPS_shop,
	klaviyo_dimensions.opt_to_email,
	ref.referral,
	shopify_dimensions.email,
	shopify_dimensions.total_price as first_purchase_price,
	shopify_dimensions.basket,
	shopify_dimensions.order_number,
	shopify_dimensions.order_incl_bottle,
	shopify_dimensions.order_incl_starter_set,
	shopify_dimensions.order_incl_colored_bottle,
	shopify_dimensions.order_incl_bundle,
	shopify_dimensions.order_incl_wildberry,
	shopify_dimensions.order_incl_other_flavour,
	shopify_dimensions.number_of_bottle ,
	shopify_dimensions.number_of_product,
	shopify_dimensions.created_at,
	datediff(day,
	klaviyo_dimensions.first_purchase_date,
	shopify_dimensions.created_at) as date_diff_klaviyo_shopify
from
	klaviyo_dimensions
left join shopify_dimensions
	on shopify_dimensions.email = klaviyo_dimensions.email
left join referer_customer as ref 
	on klaviyo_dimensions.customer_id = ref.person_id),
                                                               get_first_time_order as
--this cte provides the information of the first time orders. In case customer placed multiple orders within +-1 day of the 
-- first purchase date (nps), the first time orders will be the ealiest order
  (
select
	*
from
	(
	select
		customer_id,
		custom_nps_1,
		custom_nps_2,
		country,
		email,
		customer_duration_days,
		first_purchase_date,
		first_purchase_price,
		basket as first_basket,
		order_number,
		NPS_shop,
		opt_to_email,
		referral,
		row_number() over (partition by customer_id
	order by
		created_at) as rank_order,--the rank here only consider order with date_diff_klaviyo_shopify between -1 and 1
		order_incl_bottle,
		order_incl_starter_set,
		order_incl_colored_bottle,
		order_incl_bundle,
		order_incl_wildberry,
		order_incl_other_flavour,
		number_of_bottle ,
		number_of_product,
		created_at
	from
		all_order_from_klaviyo_customer
	where
		date_diff_klaviyo_shopify between -1 and 1)
where
	rank_order = 1),
                                                        first_time_customer as
--this etc classifies if a customer is actually the the first time customer based on the first purchase date from nps
  (
select
	customer_id,
	case
		when SUM(case
                           when date_diff_klaviyo_shopify <-1 then 1
                           else 0
                       end) = 0 then 1
		---we consider 1st purchase order is the first order placed within +-1 day from the first purchase date in Klaviyo
		else 0
	end as first_time_customers
from
	all_order_from_klaviyo_customer
group by
	customer_id),
                                                               email as
--this cte contains list of email from customers who contacted us via freshdesk
  (
select
	lower(email) as email
from
	freshdesk.conversation_to_email
group by
	lower(email)),
                                                               onboarding_campaign as
-- this cte contains the customer_id who joined our onboarding campaign
  (
select
	person_id,
	sum(unique_opens) as unique_opens,
	sum(unique_clicks) as unique_clicks
from
	"airup_eu_dwh"."klaviyo_global"."fct_click_open_per_customer"
where
	flow_id in (
		select
			distinct id
		from
			klaviyo_global.dim_flow df
		where
			name like 'NPS email 1st purchase%'
			and status = 'live')
group by
	person_id),
                                                               delivery_data as
--this cte contains information of delivery time
  (
select
	shopify_order_nr,
	delivery_time
from
	(
	select
		shopify_order_nr,
		delivery_time,
		row_number() over (partition by shopify_order_nr
	order by
		delivery_time) as rank_order
	from
		"airup_eu_dwh"."parcelperform"."dim_parcperf_delvd_shipments")
where
	rank_order = 1),
                                                               nps1_action as
--this cte contains the timestamps of nps1 events
  (
select
	person_id,
	"datetime",
	"type"
from
	(
	select
		person_id,
		"datetime",
		"type",
		row_number() over (partition by person_id,
		"type"
	order by
		datetime) as rank_action
	from
		"airup_eu_dwh"."klaviyo_global"."dim_event"
	where
		flow_id in (
		select
			distinct id
		from
			klaviyo_global.dim_flow df
		where
			name like 'NPS email 1st purchase%'
			and status = 'live'))
where
	rank_action = 1),
                                                               nps1_pivot as
---get the necessary format                                                           
  (
select
	*
from
	(
	select
		person_id,
		"datetime",
		"type"
	from
		nps1_action) PIVOT (min("datetime")
                               for "type" in ('Opened Email', 'Received Email', 'Clicked Email'))),
                  											nps2_action as
--this cte contains the timestamps of nps2 events                                                              
  (
select
	person_id,
	"datetime",
	"type"
from
	(
	select
		person_id,
		"datetime",
		"type",
		row_number() over (partition by person_id,
		"type"
	order by
		datetime) as rank_action
	from
		"airup_eu_dwh"."klaviyo_global"."dim_event"
	where
		flow_id in (
		select
			id
		from
			klaviyo_global.dim_flow df
		where
			name like 'NPS email 60%'
			and status = 'live'))
where
	rank_action = 1),
														 nps2_pivot as
---get the necessary format
  (
select
	*
from
	(
	select
		person_id,
		"datetime",
		"type"
	from
		nps2_action) PIVOT (min("datetime")
                               for "type" in ('Opened Email', 'Received Email', 'Clicked Email'))),                               					
                        get_order_after_nps1 as
--this cte contains orders information which customer placed between first klaviyo orders and nps2
  (
select
	customer_id,
	country
from
	(
	select
		aofkc.customer_id,
		aofkc.country,
		aofkc.basket,
		aofkc.created_at,
		gfto.created_at as actual_first_order,
		datediff(day,
		gfto.created_at,
		aofkc.created_at) as date_diff_first_order
	from
		all_order_from_klaviyo_customer as aofkc
	left join get_first_time_order gfto
  on
		aofkc.customer_id = gfto.customer_id)
where
	date_diff_first_order >0
	--check if order was placed after first klaviyo order
group by
	customer_id,
	country),
                                                               final_model as
  (
select
	gfto.customer_id,
	gfto.custom_nps_1,
	gfto.custom_nps_2,
	gfto.country,
	gfto.customer_duration_days,
	gfto.first_purchase_price,
	gfto.first_purchase_date,
	gfto.first_basket,
	gfto.order_number,
	gfto.order_incl_bottle,
	gfto.order_incl_starter_set,
	gfto.order_incl_colored_bottle,
	gfto.order_incl_bundle,
	gfto.order_incl_wildberry,
	gfto.order_incl_other_flavour,
	gfto.number_of_bottle,
	gfto.number_of_product,
	gfto.NPS_shop,
	gfto.opt_to_email,
	gfto.referral,
	first_time_customers,
	dd.delivery_time::date as delivery_time,
	case
		when m.email is not null then 1
		else 0
	end as contact_customer_service_via_email,
	case
		when oc.unique_opens >0 then 1
		else 0
	end as open_crm_onboarding_email,
	case
		when oc.unique_clicks >0 then 1
		else 0
	end as click_crm_onboarding_email,
	nps1."opened email" as nps1_opened_email,
	nps1."received email" as nps1_received_email,
	nps1."clicked email" as nps1_clicked_email,
	nps2."opened email" as nps2_opened_email,
	nps2."received email" as nps2_received_email,
	nps2."clicked email" as nps2_clicked_email,
	case
		when order_after_nps1.customer_id is not null then 1
		else 0
	end as purchased_after_nps1
from
	get_first_time_order gfto
left join first_time_customer ftc on
	gfto.customer_id = ftc.customer_id
left join email m on
	gfto.email = m.email
left join onboarding_campaign oc on
	gfto.customer_id = oc.person_id
left join delivery_data dd on
	gfto.order_number = dd.shopify_order_nr
left join nps1_pivot as nps1 on
	gfto.customer_id = nps1.person_id
left join nps2_pivot as nps2 on
	gfto.customer_id = nps2.person_id
left join get_order_after_nps1 as order_after_nps1 on
	gfto.customer_id = order_after_nps1.customer_id
	and gfto.country = order_after_nps1.country)
select
	final_model.*,
	case
		when country_abbreviation is not null then country_abbreviation
		else 'Others'
	end as market,
	case
		when country_grouping is not null then country_grouping
		else 'Others'
	end as region
from
	final_model
left join "airup_eu_dwh"."public"."country_system_account_mapping" country_mapping 
on
	final_model.country = country_mapping.country_fullname