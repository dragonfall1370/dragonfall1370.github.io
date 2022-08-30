-- legacy view
-- migrated by T. Kristof (Feld M)
-- Last Modified by: YuShih Hsieh --> change data source to "adwords_custom_new_api"



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
			   WHEN am.country = 'BE'::text THEN 'BE'::text
			   ELSE 'Other'::text
			   END AS country, 
		   round(sum(cr.cost_micros) /1000000::numeric, 0)  AS media_spend
		--FROM adwords_custom_new_api.custom_report cr
		FROM  "airup_eu_dwh"."adwords_custom_new_api"."custom_report" cr
				 --LEFT JOIN airup_eu_dwh.adwords.dim_adwords_account_mapping am USING (customer_id)
                 LEFT JOIN "airup_eu_dwh"."adwords"."dim_adwords_account_mapping" am USING (customer_id)
	WHERE am.account_type = 'youtube'::text
	AND cr.ad_network_type::text = 'YOUTUBE_WATCH'::text OR cr.ad_network_type::text = 'YOUTUBE_SEARCH'::text
	GROUP BY 1,2,3,4
),
	 youtube_cost AS (
		 SELECT youtube_cost_preparation.date,
		 		youtube_cost_preparation.country,
				sum(youtube_cost_preparation.media_spend) AS media_spend
		 FROM youtube_cost_preparation
		 WHERE youtube_cost_preparation.channel = 'google'::text
		   AND youtube_cost_preparation.platform = 'youtube'::text
		   AND (youtube_cost_preparation.country = ANY
				(ARRAY ['AT'::text, 'DE'::text, 'FR'::text, 'NL'::text, 'CH'::text, 'UK'::text, 'IT'::text, 'SE'::text, 'BE'::text]))
		 GROUP BY 1,2
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
		  FROM "airup_eu_dwh"."exit_survey"."youtube_survey_data_scaled" youtube_survey_data_scaled
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
		  FROM "airup_eu_dwh"."exit_survey"."youtube_survey_data_scaled" youtube_survey_data_scaled
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