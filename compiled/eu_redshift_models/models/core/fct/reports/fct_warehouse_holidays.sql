---Authors: Nham Dao
---Last Modified by: Nham Dao

---###################################################################################################################

        --- This view contains holidays of warehouse

---###################################################################################################################
 
select case when holidays like '%-%' then to_date(holidays ,'yyyy-mm-dd',false) else null end as holidays, warehouse 
from "airup_eu_dwh"."reports"."warehouse_holidays_manual"