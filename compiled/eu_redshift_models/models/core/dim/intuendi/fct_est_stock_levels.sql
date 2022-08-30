--- creator: Oscar Higson Spence
--- summary: view for intuendi sales data



SELECT est_stock_levels.date,
    est_stock_levels.location,
    est_stock_levels.report_date,
    est_stock_levels.sku,
    est_stock_levels.high,
    est_stock_levels.low,
    est_stock_levels.mean
   FROM "airup_eu_dwh"."intuendi"."est_stock_levels"