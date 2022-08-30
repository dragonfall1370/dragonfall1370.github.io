-- order_placed, revenue, Successful_Deliveriess
with test as (
select date(cr.date), cr.campaign_id, c.name as campaign_name,
        sum(cr.order_count) as orders, 
        sum(cr.revenue) as revenue,
        sum(cr.eur_revenue) as revenue_eur
from "airup_eu_dwh"."klaviyo_global"."fct_campaign_revenue_eur" cr
left join "airup_eu_dwh"."klaviyo_global"."dim_campaign" c
on cr.campaign_id = c.id
WHERE cr.metric_id IN ('W6xipc','SGUEKE','Tsnt2g','T6pvXU','VubSkw','UQQ8CB','TNKAfF', 'Wdbk5M')
group by 1,2,3
order by date desc