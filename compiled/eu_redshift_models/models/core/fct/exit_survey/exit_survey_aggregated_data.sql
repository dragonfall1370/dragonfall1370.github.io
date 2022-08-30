-- legacy view
-- migrated by T. Kristof (Feld M)




WITH orders_timeline AS (
		SELECT btrim(lower(fct_order_enriched_ngdpr.email::text)) AS email,
			   date(fct_order_enriched_ngdpr.created_at) AS order_date,
			   fct_order_enriched_ngdpr.customer_id,                                                                                                                          
			   fct_order_enriched_ngdpr.order_number,
			   row_number() OVER (PARTITION BY (btrim(lower(fct_order_enriched_ngdpr.email::text))) ORDER BY (date(fct_order_enriched_ngdpr.created_at)), fct_order_enriched_ngdpr.order_number) AS order_index,
			   fct_order_enriched_ngdpr.shipping_address_country                                                                                                                    AS shipping_country
		-- FROM shopify_global.fct_order_enriched_ngdpr
		FROM "airup_eu_dwh"."shopify_global"."fct_order_enriched_ngdpr" fct_order_enriched_ngdpr
		WHERE date(fct_order_enriched_ngdpr.created_at) >= '2020-05-10'::date
	),
		 answers_timeline AS (
			 SELECT post_purchase_survey._line,
					btrim(lower(post_purchase_survey.email::text))                                                           AS survey_email,
					btrim(lower(post_purchase_survey.name::text))                                                            AS survey_name,
					btrim(lower(post_purchase_survey.answer::text))                                                          AS answer,
					row_number() OVER (PARTITION BY (btrim(lower(post_purchase_survey.email::text))) ORDER BY post_purchase_survey._line) AS answer_index,
					count(*) OVER (PARTITION BY (btrim(lower(post_purchase_survey.email::text))))                                     AS answers_per_email
			  --FROM exit_survey.post_purchase_survey
			  FROM "airup_eu_dwh"."exit_survey"."post_purchase_survey" post_purchase_survey
	),

		 merged_data AS (
			 SELECT orders_timeline.email,
					orders_timeline.order_date,
					orders_timeline.customer_id,
					orders_timeline.order_number,
					orders_timeline.shipping_country,
					answers_timeline.answer,
					-- commented out values are not really needed in the dashboard or any other exposures
					CASE
						WHEN answers_timeline.answer ~ '.*youtube.*'::text THEN 'youtube'::text
						--WHEN answers_timeline.answer ~ '.*amazon.*'::text THEN 'amazon'::text
						WHEN answers_timeline.answer ~ '.*tv.*'::text THEN 'tv'::text
						WHEN answers_timeline.answer ~ '.*facebook.*'::text THEN 'facebook'::text
						WHEN answers_timeline.answer ~ '.*insta.*'::text THEN 'insta'::text
						WHEN answers_timeline.answer ~ '.*tik( )?tok.*'::text THEN 'tiktok'::text
						WHEN answers_timeline.answer ~ '.*radio.*'::text THEN 'radio'::text
						--WHEN answers_timeline.answer ~ '.*google.*'::text THEN 'google'::text
						WHEN answers_timeline.answer ~ '.*aldi.*'::text THEN 'aldi'::text
						--WHEN answers_timeline.answer ~ '.*rossmann.*'::text THEN 'rossmann'::text
						WHEN answers_timeline.answer ~ '.*freund.*|.*ami\\(e\\).*|.*ami\(e\).*|.*friend.*|.*vrienden of familie.*|.*vrienden.*|.*vriend.*|.*familj eller vänner.*|.*tramite un amico.*'::text THEN 'friend'::text
						--WHEN answers_timeline.answer ~ '.*rewe.*'::text THEN 'rewe'::text
						--WHEN answers_timeline.answer ~ '.*edeka.*'::text THEN 'edeka'::text
						--WHEN answers_timeline.answer ~ '.*faz.*|.*faz.*|.*frankfurt(er)?.*'::text THEN 'faz'::text
						WHEN answers_timeline.answer ~ '.*snap.*'::text THEN 'snapchat'::text
						WHEN answers_timeline.answer IS NOT NULL THEN 'other'::text
						ELSE NULL::text
						END                                                                                AS answer_mapped,
					orders_timeline.order_index,
					answers_timeline.answer_index,
					max(answers_timeline.answers_per_email) OVER (PARTITION BY orders_timeline.email)                                              AS max_answers_per_email,
					max(CASE WHEN answers_timeline.answer IS NOT NULL THEN orders_timeline.order_date ELSE NULL::date END)
						OVER (PARTITION BY orders_timeline.email ORDER BY orders_timeline.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)          AS max_order_date_with_answer
			 FROM orders_timeline
					  LEFT JOIN answers_timeline ON orders_timeline.email = answers_timeline.survey_email AND
													orders_timeline.order_index = answers_timeline.answer_index
		 ),

		 merged_data_enriched AS (
			 SELECT merged_data.email,
			 		merged_data.customer_id,
					merged_data.order_date,
					merged_data.order_number,
					merged_data.shipping_country,
					merged_data.answer,
					merged_data.answer_mapped,
					merged_data.order_index,
					merged_data.answer_index,
					merged_data.max_answers_per_email,
					merged_data.max_order_date_with_answer,
					CASE WHEN (merged_data.order_date - merged_data.max_order_date_with_answer) <= 30 THEN nth_value(
																										  merged_data.answer_mapped,
																										  merged_data.max_answers_per_email::integer)
																										  OVER (PARTITION BY merged_data.email ORDER BY merged_data.order_index ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
						ELSE NULL::text
						END AS latest_answer
			 FROM merged_data
		 ),

-- ###########################
-- #### qualtrics data is being introduced into the dataset below
-- #### the CTEs no. 2 & 4 above will have to be sunset and materilized as a table once qualtrics is rolled out across all the markets
-- ###########################

		merged_data_enriched_qualtrics as (
			SELECT
				merged_data_enriched.email,
				merged_data_enriched.customer_id,
				merged_data_enriched.order_date,
				merged_data_enriched.order_number,
				merged_data_enriched.shipping_country,
				qualtrics.response_mapped as qualtrics_answer,
				-- shift of logic between old exit survey and qualtrics
				-- with qualtrics it is only being filled out once and thus the case statement below
				-- when customer_id is available; use this and change join to customer_id coalesce(merged_data_enriched.answer_mapped, qualtrics.response_mapped) as answer_mapped,
				CASE
					WHEN qualtrics.is_qualtrics_response is TRUE THEN
						MAX(qualtrics.response_mapped) over (partition by merged_data_enriched.customer_id)
					ELSE
						merged_data_enriched.answer_mapped
					END
				as answer_mapped,
				merged_data_enriched.order_index,
				merged_data_enriched.answer_index,
				merged_data_enriched.max_answers_per_email,
				merged_data_enriched.max_order_date_with_answer,
				CASE
					WHEN qualtrics.is_qualtrics_response is TRUE THEN
						MAX(qualtrics.response_mapped) over (partition by merged_data_enriched.customer_id)
					ELSE
						merged_data_enriched.latest_answer
					END
				as latest_answer
				--coalesce(merged_data_enriched.latest_answer, qualtrics.response_mapped) as latest_answer
			FROM
				merged_data_enriched
				left join "airup_eu_dwh"."qualtrics"."dim_qualtrics_exit_survey" qualtrics
					on lower(merged_data_enriched.order_number) = lower(qualtrics.order_number)
		),


-- ###########################
-- #### the following CTEs are replacing CUBE function on redshift
-- ###########################


		 main_groupby AS (
			 SELECT
    				merged_data_enriched.order_date,
					COALESCE(merged_data_enriched.answer_mapped, merged_data_enriched.latest_answer,'no response'::text) AS answer, -- dim 2
					CASE
						WHEN (COALESCE(merged_data_enriched.answer_mapped, merged_data_enriched.latest_answer, 'no response'::text) = ANY (ARRAY ['youtube'::text, 'tv'::text]))
    					AND merged_data_enriched.shipping_country::text = 'Germany'::text THEN GREATEST(count(DISTINCT merged_data_enriched.order_number) - 6, 0::bigint)
						ELSE count(DISTINCT merged_data_enriched.order_number)
					END 							AS orders,

					count(DISTINCT merged_data_enriched.email) AS customers,
					shipping_country -- dim 1
			 FROM merged_data_enriched_qualtrics merged_data_enriched
			 GROUP BY
    				order_date,
    				shipping_country, -- dim 1
    				(COALESCE(answer_mapped, latest_answer,'no response'::text)) -- dim 2
		 ),
		 
		 country_groupby AS (
			 SELECT
    			order_date,
    			'total' as answer,
    			shipping_country,
    			sum(orders)::bigint as orders,
				sum(customers)::bigint  as customers
    		 FROM
    			main_groupby
    		 GROUP BY
    			1,2,3
		 ),

		 answer_groupby AS (
			 SELECT
    			order_date,
    			answer,
    			'All' as shipping_country,
    			sum(orders)::bigint as orders,
				sum(customers)::bigint  as customers
    		 FROM
    			main_groupby
    		 GROUP BY
    			1,2,3
		 ),

		 total_groupby AS (
			 SELECT
    			order_date,
    			'total' as answer,
    			'All' as shipping_country,
    			sum(orders)::bigint as orders,
				sum(customers)::bigint as customers
    		 FROM
    			main_groupby
    		 GROUP BY
    			1,2,3
		 ),

    	union_cte as (
    		select
			   order_date,
			   answer,
			   orders::bigint,
			   customers::bigint,
			   shipping_country
    		from main_groupby
			union all
    		select
 			   order_date,
			   answer,
			   orders::bigint,
			   customers::bigint,
			   shipping_country
    		from country_groupby
    		union all
    		select
    		   order_date,
			   answer,
			   orders::bigint,
			   customers::bigint,
			   shipping_country
    		from answer_groupby
    		union all
    		select
    		   order_date,
			   answer,
			   orders::bigint,
			   customers::bigint,
			   shipping_country
    		from total_groupby
		)
select
	union_cte.order_date,
	union_cte.answer,
	union_cte.orders,
	union_cte.customers,
	union_cte.shipping_country,
    rollout_dates.*
from
	union_cte
	left join "airup_eu_dwh"."exit_survey"."qualtrics_rollout_dates" rollout_dates 
		on union_cte.shipping_country = rollout_dates.country