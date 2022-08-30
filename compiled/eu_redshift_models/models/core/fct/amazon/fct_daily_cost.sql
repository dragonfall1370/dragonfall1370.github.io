----legacy: amazon.daily_cost
---Authors: Etoma Egot
---Last Modified by: Tomas Kristof

---###################################################################################################################

        ---compute amazon.daily_cost---

---###################################################################################################################
 

select
    "date" as reporting_date,
    month_classification,
    refresh_date,
    null as mws_marketing_channel,
    net_revenue as revenue,
    0 as cost,
    "date" >= date_trunc('quarter', current_date) as this_quarter
from
    "airup_eu_dwh"."amazon"."fct_mws_daily_net_revenue"