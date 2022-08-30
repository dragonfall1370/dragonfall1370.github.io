--- creator: Nham Dao
--- last modify: Nham Dao

with 
inbound_forecast_clean as (
SELECT *
FROM (SELECT supplier, pol, region, pod, container_type,"_01_01_2022",
"_01_02_2022",
"_01_03_2022",
"_01_04_2022",
"_01_05_2022",
"_01_06_2022",
"_01_07_2022",
"_01_08_2022",
"_01_09_2022",
"_01_10_2022",
"_01_11_2022",
"_01_12_2022",
"_01_01_2023",
"_01_02_2023",
"_01_03_2023",
"_01_04_2023",
"_01_05_2023",
"_01_06_2023",
"_01_07_2023",
"_01_08_2023",
"_01_09_2023",
"_01_10_2023",
"_01_11_2023",
"_01_12_2023",
"_01_01_2024",
"_01_02_2024",
"_01_03_2024",
"_01_04_2024",
"_01_05_2024",
"_01_06_2024",
"_01_07_2024",
"_01_08_2024",
"_01_09_2024",
"_01_10_2024",
"_01_11_2024",
"_01_12_2024"
 FROM "airup_eu_dwh"."logistics_inbound"."inbound_forecast_manual") UNPIVOT (
    qty FOR "date" IN ("_01_01_2022",
"_01_02_2022",
"_01_03_2022",
"_01_04_2022",
"_01_05_2022",
"_01_06_2022",
"_01_07_2022",
"_01_08_2022",
"_01_09_2022",
"_01_10_2022",
"_01_11_2022",
"_01_12_2022",
"_01_01_2023",
"_01_02_2023",
"_01_03_2023",
"_01_04_2023",
"_01_05_2023",
"_01_06_2023",
"_01_07_2023",
"_01_08_2023",
"_01_09_2023",
"_01_10_2023",
"_01_11_2023",
"_01_12_2023",
"_01_01_2024",
"_01_02_2024",
"_01_03_2024",
"_01_04_2024",
"_01_05_2024",
"_01_06_2024",
"_01_07_2024",
"_01_08_2024",
"_01_09_2024",
"_01_10_2024",
"_01_11_2024",
"_01_12_2024")))
select supplier, pol, region, pod, container_type,
cast (qty as float) as qty,
to_date(replace(right("date",10), '_', '.'), 'dd/mm/yyyy', false) from inbound_forecast_clean
where supplier is not null