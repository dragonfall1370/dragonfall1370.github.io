--- creator: Oscar Higson Spence
--- summary: view for intuendi sales data



SELECT est_planning.location,
    est_planning.provider,
    est_planning.reorder_date,
    est_planning.report_date,
    est_planning.sku,
    est_planning.amount,
    est_planning.eta,
    est_planning.quantity,
    est_planning.estimated_coverage,
    est_planning.type
   FROM "airup_eu_dwh"."intuendi"."est_planning"