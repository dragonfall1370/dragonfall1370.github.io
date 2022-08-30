--- creator: Nham Dao
--- last modify: Nham Dao


select region, month, to_number("value", '99D99') as "value" 
from "airup_eu_dwh"."google_analytics"."seed_1st_purchase_conversion_rate"