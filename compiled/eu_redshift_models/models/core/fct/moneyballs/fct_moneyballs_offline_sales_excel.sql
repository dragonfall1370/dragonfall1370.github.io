 

SELECT 'Central Europe'::text AS region,
    'Germany'::text AS country,
    offline_sales_excel.date,
    sum(offline_sales_excel.net_revenue) AS net_revenue
   FROM "airup_eu_dwh"."odoo"."seed_offline_sales_excel" offline_sales_excel
  GROUP BY 1, 2, 3