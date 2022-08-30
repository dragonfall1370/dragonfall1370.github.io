--created by: Nham Dao



with event_data as (SELECT person_id , case when flow_id in (
		select
			distinct id
		from
			klaviyo_global.dim_flow df
		where
			name like 'NPS email 1st purchase%'
			and status = 'live') then 1 end as NPS1,
              case when flow_id in (
		select
			distinct id
		from
			klaviyo_global.dim_flow df
		where
			name like 'NPS email 60%'
			and status = 'live') then 1 end as NPS2,
             max(
                CASE
                    WHEN e."type" = 'Received Email' OR e."type" = 'Received Email (MailChimp)' THEN 1
                    ELSE 0
                END) AS delivered, 
             max(
                CASE
                    WHEN e."type" = 'Opened Email' OR e."type" = 'Opened Email (MailChimp)' THEN 1
                    ELSE 0
                END) AS opens, 
             max(
                CASE
                    WHEN e."type" = 'Clicked Email' OR e."type" = 'Clicked Email (MailChimp)' THEN 1
                    ELSE 0
                END) AS clicks
FROM klaviyo_global.dim_event e
WHERE flow_id in (
		select
			id
		from
			klaviyo_global.dim_flow df
		where
			(name like 'NPS email 60%' or name like 'NPS email 1st purchase%')
			and status = 'live')
group by person_id,NPS1,NPS2) , 
prepare_data as 
(select ed.*, dp1.custom_nps_1, dp2.custom_nps_2 from event_data ed
left join klaviyo_global.dim_person dp1
on ed.person_id = dp1.id 
and ed.NPS1 = 1 and dp1.custom_nps_1 is not null and dp1.custom_first_purchase_date::date >='2021-09-14'
left join klaviyo_global.dim_person dp2
on ed.person_id = dp2.id 
and ed.NPS2 = 1 and dp2.custom_nps_2 is not null and dp2.custom_first_purchase_date::date >='2021-09-14' )
select sum(delivered) as delivered, sum(opens) as opens, sum(clicks) as clicks,
sum(case when custom_nps_1 is not null then 1 else 0 end ) as response_nps1, 
sum(case when custom_nps_2 is not null then 1 else 0 end ) as response_nps2,
case when NPS1 = 1 then 'NPS1'
when NPS2 = 1 then 'NPS2' end as NPS
from prepare_data group by NPS1, NPS2