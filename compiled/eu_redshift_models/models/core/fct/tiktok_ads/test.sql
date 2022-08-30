--- Authors: YuShih Hsieh
--- Last Modified by: YuShih Hsieh

--- ###################################################################################################################
        -- this view contains the daily ads spend pre country from TikTok channel
--- ###################################################################################################################




select crd.stat_time_day:: date as date, ftam.country_abbreviation as region, 
	'Paid Social'::text AS channel,
	'TikTok'::text AS channel_subcategory,
	sum(crd.spend) as total_spend
from "airup_eu_dwh"."tiktok_ads"."campaign_report_daily" crd
left join "airup_eu_dwh"."tiktok_ads"."campaign_history" ch
on crd.campaign_id = ch.campaign_id 
left join "airup_eu_dwh"."tiktok_ads"."fct_tiktok_account_mapping" ftam
on ch.advertiser_id = ftam.id
group by 1,2,3,4
order by 1 desc