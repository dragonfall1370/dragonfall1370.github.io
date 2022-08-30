--created by: Etoma Egot



SELECT engagement.date,
    engagement.group_name,
    engagement.metric_name,
    engagement.metric_value,
    engagement._fivetran_synced AS etl_loaded_at
FROM "airup_eu_dwh"."officevibe"."engagement" engagement