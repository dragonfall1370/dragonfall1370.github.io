---Authors: Abhishek Pathak
---Last Modified by: Abhishek Pathak

---###################################################################################################################
        -- This view contains the information on the RFM based customer segmenation.
---###################################################################################################################



-- This model replicates the RFM matrix for customer segmentation which was done as part of the CLV project.
-- The detailed information about the matrix and how customers are segmented according to it is available in this asana task (https://app.asana.com/0/0/1201947602926104/f).
select customer_id, 
email, 
shopify_shop, 
shop_country,
recency,
frequency,
case when recency in ('0-45', '46-90') and frequency = '1' then '1st_order_active'
when recency = '0-45' and frequency in ('3', '4', '5 & more') then 'top_active'
when (recency in ('0-45', '46-90') and frequency = '2') or (recency in ('0-45', '46-90')  and frequency in ('3', '4', '5 & more')) then 'core_active'
when recency = '91-180' then 'at_risk'
when recency = '181-364' and frequency in ('3', '4', '5 & more') then 'dormant'
when (recency = '365+') or (recency = '181-364' and frequency in ('1', '2')) then 'churned' end as customer_segment
from ( 
-- This inner or base sql pass calculates: 1. Recency (how recently a customer last bought something), grouping it then into corresponding recency bucket (at each customer level).
-- 2. Frequency i.e. how many times till now that customer has bought something from airup (at each customer level). 
select distinct customer_id, 
email, 
shopify_shop, 
shop_country,
case when current_date  - max(created_at::date) between 0 and 45 then '0-45' 
when current_date  - max(created_at::date) between 46 and 90 then '46-90'
when current_date  - max(created_at::date) between 91 and 180 then '91-180'
when current_date  - max(created_at::date) between 181and 364 then '181-364'
when current_date  - max(created_at::date)>364 then '365+'
else  (current_date  - max(created_at::date))::text end  as recency, 
case when count(*)>= 5 then '5 & more' else count(*)::text end as frequency,
sum(net_revenue_2) as net_revenue
from "airup_eu_dwh"."shopify_global"."fct_order_enriched_ngdpr" foe 
group by customer_id, email, shopify_shop, shop_country)