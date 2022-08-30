


select fp.date, fp.profile, fp.shopping_stage, SUM(fp.sessions) as sessions 
from "airup_eu_dwh"."google_analytics"."funnel_performance" fp
where (fp.shopping_stage::text = 'ALL_VISITS'::text OR fp.shopping_stage::text = 'PRODUCT_VIEW'::text OR fp.shopping_stage::text = 'ADD_TO_CART'::text OR fp.shopping_stage::text = 'TRANSACTION'::text OR fp.shopping_stage::text = 'CHECKOUT'::text or fp.shopping_stage = 'NO_SHOPPING_ACTIVITY' or fp.shopping_stage = 'NO_CART_ADDITION' or fp.shopping_stage = 'CART_ABANDONMENT' or fp.shopping_stage = 'CHECKOUT_ABANDONMENT')
and profile = '241487202'
group by 1,2,3
order by 1 desc, 4