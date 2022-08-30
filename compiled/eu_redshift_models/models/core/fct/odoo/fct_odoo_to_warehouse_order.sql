--- creator: Nham Dao
--- last modify: Nham Dao
--- summary: clean ihub_event data from 1 row multiple order into 1 row 1 order


WITH order_airup_airupuk AS (
         select details,
            ie.create_date,
            ie.summary
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
      split_part(details, '\n', s.gen_num) AS order_number_detail
    FROM order_airup_airupuk AS ts
      JOIN splitter AS s ON 1 = 1
    WHERE split_part(details, '\n', s.gen_num) <> ''
), 
order_clean as 
(SELECT split_part(expanded_input.order_number_detail, ';'::text, 1) AS order_number,
            split_part(expanded_input.order_number_detail, ';'::text, 2) AS order_index,
            expanded_input.create_date,
            expanded_input.summary
           FROM expanded_input
        )
SELECT
        CASE
            WHEN order_clean.summary = 'Transmit Sale Orders - 200 - AIRUP' THEN concat('GEOAP/OUT/', order_clean.order_number)
            ELSE concat('DCUK1/OUT/', order_clean.order_number)
        END AS order_number,
    order_clean.create_date
   FROM order_clean
  WHERE order_clean.order_index = '1' AND 
 date(order_clean.create_date) >= (date(CURRENT_DATE) - 90)