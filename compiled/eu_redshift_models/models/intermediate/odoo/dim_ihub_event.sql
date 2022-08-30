 

WITH order_airup_airupuk AS (
        select details,
          ie.create_date,
          ie.summary,
          ie._fivetran_synced
          FROM "airup_eu_dwh"."odoo"."ihub_event" ie
        WHERE ie.summary::text = ANY (ARRAY['Transmit Sale Orders - 200 - AIRUP'::character varying, 'Transmit Sale Orders - 200 - AIRUPUK'::character varying]::text[])
      ),         
splitter AS
(
  SELECT *
  FROM "airup_eu_dwh"."reports"."series_of_number"
  WHERE gen_num BETWEEN 1 AND (SELECT max(REGEXP_COUNT(details, '\n') + 1)
                                FROM order_airup_airupuk st)
)
--select * from splitter;
, expanded_input AS
(
  SELECT
    create_date,
          summary,
          _fivetran_synced,
    split_part(details, '\n', s.gen_num) AS order_number_detail
  FROM order_airup_airupuk AS ts
    JOIN splitter AS s ON 1 = 1
  WHERE split_part(details, '\n', s.gen_num) <> ''
), 
order_clean as 
(SELECT split_part(expanded_input.order_number_detail, ';'::text, 1) AS order_number,
          split_part(expanded_input.order_number_detail, ';'::text, 2) AS order_index,
          expanded_input.create_date,
          expanded_input.summary, 
          expanded_input._fivetran_synced
          FROM expanded_input
      )
select 
order_number, create_date, _fivetran_synced
from (SELECT
      CASE
          WHEN order_clean.summary = 'Transmit Sale Orders - 200 - AIRUP' THEN concat('GEOAP/OUT/', order_clean.order_number)
          ELSE concat('DCUK1/OUT/', order_clean.order_number)
      END AS order_number,
  order_clean.create_date,
  order_clean._fivetran_synced,
  row_number() over(partition by CASE
          WHEN order_clean.summary = 'Transmit Sale Orders - 200 - AIRUP' THEN concat('GEOAP/OUT/', order_clean.order_number)
          ELSE concat('DCUK1/OUT/', order_clean.order_number)
      END order by create_date desc) as row_index
  FROM order_clean
WHERE order_clean.order_index = '1')
where row_index =1

 
   and _fivetran_synced >= (select max(_fivetran_synced) from "airup_eu_dwh"."odoo"."dim_ihub_event")
  