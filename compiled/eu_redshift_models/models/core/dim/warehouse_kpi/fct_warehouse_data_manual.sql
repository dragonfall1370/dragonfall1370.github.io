--- creator: Nham Dao
--- last modify: Nham Dao
--- summary: warehouse data (manual from excel files)

select year, week, date, dpmo, target, sheet_name, file_name, refreshed_time
from "airup_eu_dwh"."warehouse_kpi"."warehouse_data_manual"