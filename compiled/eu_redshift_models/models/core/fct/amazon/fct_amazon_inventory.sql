--created by: Nham Dao
--amazon inventory data

 

select asin, afn_warehouse_quantity as quantity_on_hand, afn_reserved_quantity as reserved_quantity, 'Amazon DE' as warehouse, Null as location, 
round(cast(your_price as float)*(cast(afn_warehouse_quantity as float)-cast(afn_reserved_quantity as float)),2) as value, created_at::date as "timestamp" 
from "airup_eu_dwh"."amazon"."fba_managed_inventory"