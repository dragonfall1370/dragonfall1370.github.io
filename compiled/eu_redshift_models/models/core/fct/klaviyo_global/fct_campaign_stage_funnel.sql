---Authors: YuShih Hsieh
---Last Modified by: YuShih Hsieh

---###################################################################################################################
        -- this view contains the information on the CRM campaign stages: 
        -- namely Emails delivered, Emails opened, Emails clicked, Orders placed
        -- data sources are from klaviyo_global & klaviyo_campaign_revenue
---###################################################################################################################





with campaign_orders as (
    select date(cv.send_time) as date, cv.campaign_id, cv.campaign_name,
            'Orders placed'::text as campaign_stage,
            sum(cv.placed_order) as orders, 
            sum(cv.revenue) as revenue, 
            sum(cv.successful_deliveries) as successful_deliveries
    from "airup_eu_dwh"."klaviyo_global"."fct_campaign_view" cv
    group by 1,2,3
), campaign_stage as (
    select date(c.send_time) as date, dp.id as customer_id, c.id as campaign_id, c.name as campaign_name,
        Case when e.type = 'Opened Email' OR e.type= 'Opened Email (MailChimp)' then 'Emails opened'
             when e.type = 'Clicked Email' OR e.type = 'Clicked Email (MailChimp)' then 'Emails clicked'
             when e.type = 'Received Email' OR e.type = 'Received Email (MailChimp)' then 'Emails received'
            when e.type = 'Dropped Email' then 'Emails dropped'
        End as campaign_stage               
    FROM "airup_eu_dwh"."klaviyo_global"."dim_event" e
    JOIN "airup_eu_dwh"."klaviyo_global"."dim_campaign" c ON e.campaign_id = c.id
    JOIN "airup_eu_dwh"."klaviyo_global"."dim_person" dp ON e.person_id = dp.id
    group by 1,2,3,4,5
), campaign_stage_value as (
    select date, campaign_id, campaign_name, campaign_stage,
        Case when campaign_stage = 'Emails opened' then  SUM(case when campaign_stage = 'Emails opened' then 1 else 0 end) 
            when  campaign_stage =  'Emails clicked' then SUM(case when campaign_stage = 'Emails clicked' then 1 else 0 end) 
            when  campaign_stage =  'Emails received' then SUM(case when campaign_stage = 'Emails received' then 1 else 0 end)
            when  campaign_stage =  'Emails dropped' then SUM(case when campaign_stage = 'Emails dropped' then 1 else 0 end)        
        end as value,
        ''::double precision as revenue,
        ''::double precision as successful_deliveries
    from campaign_stage
    where campaign_stage is not null
    group by 1,2,3,4
), campaign_stage_email_delivered as ( 
    select date, campaign_id, campaign_name, 'Emails delivered'::text as campaign_stage,
            (SUM(case when campaign_stage = 'Emails received' then value 
                ELSE NULL::bigint
                end) - 
            COALESCE(SUM(case when campaign_stage = 'Emails dropped' then value
                ELSE NULL::bigint
                end),0)) as value,
        ''::double precision as revenue,
        ''::double precision as successful_deliveries
    from campaign_stage_value
    group by 1,2,3
)
-- combine all campaign stages     
    select date, campaign_id, campaign_name, campaign_stage, value, revenue, successful_deliveries
    from campaign_stage_value
    union 
    select date, campaign_id, campaign_name, campaign_stage, value, revenue, successful_deliveries
    from campaign_stage_email_delivered
    where value is not null 
    union 
    select date, campaign_id, campaign_name, campaign_stage, orders as value, revenue, successful_deliveries
    from campaign_orders