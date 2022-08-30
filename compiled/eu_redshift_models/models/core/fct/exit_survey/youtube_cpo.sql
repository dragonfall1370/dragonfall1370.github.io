-- legacy view
-- migrated by T. Kristof (Feld M)
-- changed source for adwords_custom from fct to raw table (Abhishek Pathak, 10-Mar-2022)



WITH youtube_cost_preparation AS (
	SELECT cr.date,
		   'google'::text                  AS channel,
		   am.account_type                 AS platform,
		   CASE
			   WHEN am.country = 'AT'::text THEN 'AT'::text
			   WHEN am.country = 'DE'::text THEN 'DE'::text
			   WHEN am.country = 'FR'::text THEN 'FR'::text
			   WHEN am.country = 'NL'::text THEN 'NL'::text
			   WHEN am.country = 'CH'::text THEN 'CH'::text
			   WHEN am.country = 'UK'::text THEN 'UK'::text
			   WHEN am.country = 'IT'::text THEN 'IT'::text
			   WHEN am.country = 'SE'::text THEN 'SE'::text
			   ELSE 'Other'::text
			   END                         AS country,
		   round(sum(cr.cost)::numeric, 0) AS media_spend
		--FROM adwords_custom.fct_adwords_custom_report cr
		 FROM "airup_eu_dwh"."adwords_custom"."custom_report" cr
				 -- LEFT JOIN airup_eu_dwh.adwords.dim_adwords_account_mapping am USING (customer_id)
                 LEFT JOIN "airup_eu_dwh"."adwords"."dim_adwords_account_mapping" am USING (customer_id)
	WHERE am.account_type = 'youtube'::text
	  AND cr.ad_network_type_1::text = 'YouTube Videos'::text
	GROUP BY cr.date, am.account_type,
			 (
				 CASE
					 WHEN am.country = 'AT'::text THEN 'AT'::text
					 WHEN am.country = 'DE'::text THEN 'DE'::text
					 WHEN am.country = 'FR'::text THEN 'FR'::text
					 WHEN am.country = 'NL'::text THEN 'NL'::text
					 WHEN am.country = 'CH'::text THEN 'CH'::text
					 WHEN am.country = 'UK'::text THEN 'UK'::text
					 WHEN am.country = 'IT'::text THEN 'IT'::text
					 WHEN am.country = 'SE'::text THEN 'SE'::text
					 ELSE 'Other'::text
					 END)
),
	 youtube_cost AS (
		 SELECT youtube_cost_preparation.date,
				CASE
					WHEN youtube_cost_preparation.country = 'NL'::text THEN 'NL, BE'::text
					ELSE youtube_cost_preparation.country
					END                                   AS country,
				sum(youtube_cost_preparation.media_spend) AS media_spend
		 FROM youtube_cost_preparation
		 WHERE youtube_cost_preparation.channel = 'google'::text
		   AND youtube_cost_preparation.platform = 'youtube'::text
		   AND (youtube_cost_preparation.country = ANY
				(ARRAY ['AT'::text, 'DE'::text, 'FR'::text, 'NL'::text, 'CH'::text, 'UK'::text, 'IT'::text, 'SE'::text]))
		 GROUP BY youtube_cost_preparation.date,
				  (
					  CASE
						  WHEN youtube_cost_preparation.country = 'NL'::text THEN 'NL, BE'::text
						  ELSE youtube_cost_preparation.country
						  END)
	 ),
	 youtube_survey_data_scaled AS (
		 SELECT exit_survey_aggregated_data.order_date,
				CASE
					WHEN exit_survey_aggregated_data.shipping_country::text = ANY
						 (ARRAY ['Belgium'::character varying::text, 'Netherlands'::character varying::text])
						THEN 'NL, BE'::text
					WHEN exit_survey_aggregated_data.shipping_country::text = 'Austria'::text THEN 'AT'::text
					WHEN exit_survey_aggregated_data.shipping_country::text = 'Germany'::text THEN 'DE'::text
					WHEN exit_survey_aggregated_data.shipping_country::text = 'France'::text THEN 'FR'::text
					WHEN exit_survey_aggregated_data.shipping_country::text = 'Switzerland'::text THEN 'CH'::text
					WHEN exit_survey_aggregated_data.shipping_country::text = 'United Kingdom'::text THEN 'UK'::text
					WHEN exit_survey_aggregated_data.shipping_country::text = 'Italy'::text THEN 'IT'::text
					WHEN exit_survey_aggregated_data.shipping_country::text = 'Sweden'::text THEN 'SE'::text
					ELSE NULL::text
					END          AS shipping_country,
				sum(
						CASE
							WHEN exit_survey_aggregated_data.answer = 'youtube'::text
								THEN exit_survey_aggregated_data.orders
							ELSE NULL::bigint
							END) AS youtube_responses,
				sum(
						CASE
							WHEN exit_survey_aggregated_data.answer = 'total'::text
								THEN exit_survey_aggregated_data.orders
							ELSE NULL::bigint
							END) / NULLIF(sum(
												  CASE
													  WHEN exit_survey_aggregated_data.answer = 'total'::text
														  THEN exit_survey_aggregated_data.orders
													  ELSE NULL::bigint
													  END) - sum(
												  CASE
													  WHEN exit_survey_aggregated_data.answer = 'no response'::text
														  THEN exit_survey_aggregated_data.orders
													  ELSE NULL::bigint
													  END), 0::numeric) * sum(
						CASE
							WHEN exit_survey_aggregated_data.answer = 'youtube'::text
								THEN exit_survey_aggregated_data.orders
							ELSE NULL::bigint
							END) AS youtube_responses_scaled
		 --FROM exit_survey.exit_survey_aggregated_data exit_survey_aggregated_data
		 FROM "airup_eu_dwh"."exit_survey"."exit_survey_aggregated_data" exit_survey_aggregated_data
		 WHERE exit_survey_aggregated_data.shipping_country::text = ANY
			   (ARRAY ['Austria'::character varying::text, 'Germany'::character varying::text, 'Switzerland'::character varying::text, 'France'::character varying::text, 'Belgium'::character varying::text, 'Netherlands'::character varying::text, 'United Kingdom'::character varying::text, 'Italy'::character varying::text, 'Sweden'::character varying::text])
		 GROUP BY exit_survey_aggregated_data.order_date,
				  (
					  CASE
						  WHEN exit_survey_aggregated_data.shipping_country::text = ANY
							   (ARRAY ['Belgium'::character varying::text, 'Netherlands'::character varying::text])
							  THEN 'NL, BE'::text
						  WHEN exit_survey_aggregated_data.shipping_country::text = 'Austria'::text THEN 'AT'::text
						  WHEN exit_survey_aggregated_data.shipping_country::text = 'Germany'::text THEN 'DE'::text
						  WHEN exit_survey_aggregated_data.shipping_country::text = 'France'::text THEN 'FR'::text
						  WHEN exit_survey_aggregated_data.shipping_country::text = 'Switzerland'::text THEN 'CH'::text
						  WHEN exit_survey_aggregated_data.shipping_country::text = 'United Kingdom'::text THEN 'UK'::text
						  WHEN exit_survey_aggregated_data.shipping_country::text = 'Italy'::text THEN 'IT'::text
						  WHEN exit_survey_aggregated_data.shipping_country::text = 'Sweden'::text THEN 'SE'::text
						  ELSE NULL::text
						  END)
	 ),

-- ##########################
-- ### the following the CTEs are replacing postgres rollup
-- ### without union of two CTEs as rollup is not available in redshift
-- ##########################

	country_grouping as (
		 SELECT
		   youtube_survey_data_scaled.order_date,
		   sum(youtube_survey_data_scaled.youtube_responses)                                                     AS youtube_responses,
		   sum(youtube_survey_data_scaled.youtube_responses_scaled)                                              AS youtube_responses_scaled,
		   sum(youtube_cost.media_spend)                                                                         AS media_spend,
		   sum(youtube_cost.media_spend) / NULLIF(sum(youtube_survey_data_scaled.youtube_responses),
												  0::numeric)                                                    AS cpo_regular,
		   sum(youtube_cost.media_spend) / NULLIF(sum(youtube_survey_data_scaled.youtube_responses_scaled),
												  0::numeric)                                                    AS cpo_scaled,
		   youtube_survey_data_scaled.shipping_country AS shipping_country
		  FROM youtube_survey_data_scaled
		  LEFT JOIN youtube_cost
		      ON youtube_survey_data_scaled.order_date = youtube_cost.date
		      AND youtube_survey_data_scaled.shipping_country = youtube_cost.country
		  GROUP BY youtube_survey_data_scaled.order_date, youtube_survey_data_scaled.shipping_country
	      ORDER BY youtube_survey_data_scaled.order_date DESC
         ),

	all_grouping as (
		 SELECT
		   youtube_survey_data_scaled.order_date,
		   sum(youtube_survey_data_scaled.youtube_responses)                                                     AS youtube_responses,
		   sum(youtube_survey_data_scaled.youtube_responses_scaled)                                              AS youtube_responses_scaled,
		   sum(youtube_cost.media_spend)                                                                         AS media_spend,
		   sum(youtube_cost.media_spend) / NULLIF(sum(youtube_survey_data_scaled.youtube_responses),
												  0::numeric)                                                    AS cpo_regular,
		   sum(youtube_cost.media_spend) / NULLIF(sum(youtube_survey_data_scaled.youtube_responses_scaled),
												  0::numeric)                                                    AS cpo_scaled,
		   'All'::text AS shipping_country
		  FROM youtube_survey_data_scaled
		  LEFT JOIN youtube_cost
		      ON youtube_survey_data_scaled.order_date = youtube_cost.date
		      AND youtube_survey_data_scaled.shipping_country = youtube_cost.country
		  GROUP BY youtube_survey_data_scaled.order_date
	      ORDER BY youtube_survey_data_scaled.order_date DESC
         ),

     union_of_groupings as (
         select * from country_grouping
         union all
         select * from all_grouping
	 )

select
	*
from
	union_of_groupings