

SELECT ad_country_report.stat_time_day::date AS date,
    case when country_code = 'GB' then 'UK'
	    else country_code 
    end as region,  
    'Paid Social'::text AS channel,
    'TikTok'::text AS channel_subcategory,
    sum(ad_country_report.spend) AS total_spend
   FROM "airup_eu_dwh"."tiktok_ads"."ad_country_report" ad_country_report
  WHERE country_code <> 'None'  
  GROUP BY 1,2
  ORDER BY 1 DESC