--- Authors: YuShih Hsieh
--- Last Modified by: YuShih Hsieh

--- ###################################################################################################################
        -- this view contains the TikTok accont id, country country_abbreviation, country fullname and region grouping
--- ###################################################################################################################



select aa.id, 
	case when aa.name = 'air up GmbH' then 'DE'
		when aa.name = 'air up FR' then 'FR'
	 	when aa.name = 'air up CH' then 'CH'
		when aa.name = 'air up SE' then 'SE'
		when aa.name = 'air up NL' then 'NL'
		when aa.name = 'air up UK' then 'UK'
		when aa.name = 'air up IT' then 'IT'	
		when aa.name = 'air up AT' then 'AT'	
	else 'Other' 
	end as country_abbreviation,
		case when aa.name = 'air up GmbH' then 'Germany'
		when aa.name = 'air up FR' then 'France'
	 	when aa.name = 'air up CH' then 'Switzerland'
		when aa.name = 'air up SE' then 'Sweden'
		when aa.name = 'air up NL' then 'Netherlands'
		when aa.name = 'air up UK' then 'United Kingdom'
		when aa.name = 'air up IT' then 'Italy'	
		when aa.name = 'air up AT' then 'Austria'		
	else 'Other' 
	end as country_fullname,
		case when aa.name = 'air up GmbH' then 'Central Europe'
		when aa.name = 'air up FR' then 'South Europe'
	 	when aa.name = 'air up CH' then 'Central Europe'
		when aa.name = 'air up SE' then 'North Europe'
		when aa.name = 'air up NL' then 'North Europe'
		when aa.name = 'air up UK' then 'North Europe'
		when aa.name = 'air up IT' then 'South Europe'	
		when aa.name = 'air up AT' then 'Central Europe'		
	else 'Other' 
	end as country_grouping	
from "airup_eu_dwh"."tiktok_ads"."advertiser" aa