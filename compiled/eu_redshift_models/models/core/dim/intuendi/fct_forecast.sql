--- creator: Oscar Higson Spence
--- summary: view for intuendi sales data




SELECT forecast.date,
    forecast.location,
    forecast.region,
    forecast.report_date,
    forecast.sku,
    forecast.forecast::integer AS forecast,
    forecast.category,
    forecast.type
   FROM "airup_eu_dwh"."intuendi"."forecast"