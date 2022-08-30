--- creator: Oscar Higson Spence
--- summary: view for intuendi sales data



SELECT forecast_view.date,
    forecast_view.location,
    forecast_view.region,
    forecast_view.report_date,
    forecast_view.sku,
    forecast_view.forecast,
    forecast_view.category,
    forecast_view.type,
    vat_selling_price."VAT",
    vat_selling_price.selling_price,
    vat_selling_price.selling_country
   FROM "airup_eu_dwh"."intuendi"."fct_forecast" forecast_view
     LEFT JOIN intuendi.vat_selling_price ON forecast_view.sku = vat_selling_price.sku
      AND "right"(forecast_view.region::text, 2) = vat_selling_price.selling_country::text