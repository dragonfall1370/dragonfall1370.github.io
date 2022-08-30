-- legacy view
-- migrated by aman.singla (Feld M)



WITH youtube_survey_data_scaled AS (
	SELECT exit_survey_aggregated_data.order_date,
				CASE
					WHEN exit_survey_aggregated_data.shipping_country::text = 'Austria'::text THEN 'AT'::text
					WHEN exit_survey_aggregated_data.shipping_country::text = 'Germany'::text THEN 'DE'::text
					WHEN exit_survey_aggregated_data.shipping_country::text = 'France'::text THEN 'FR'::text
					WHEN exit_survey_aggregated_data.shipping_country::text = 'Switzerland'::text THEN 'CH'::text
					WHEN exit_survey_aggregated_data.shipping_country::text = 'United Kingdom'::text THEN 'UK'::text
					WHEN exit_survey_aggregated_data.shipping_country::text = 'Italy'::text THEN 'IT'::text
					WHEN exit_survey_aggregated_data.shipping_country::text = 'Sweden'::text THEN 'SE'::text
					WHEN exit_survey_aggregated_data.shipping_country::text = 'Belgium'::text THEN 'BE'::text
					WHEN exit_survey_aggregated_data.shipping_country::text = 'Netherlands'::text THEN 'NL'::text
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
							END)*1.0 / NULLIF(sum(
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
		 GROUP BY 1,2)

SELECT * FROM youtube_survey_data_scaled