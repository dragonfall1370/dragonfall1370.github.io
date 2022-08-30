

select 
    id as survey_id,
    owner_id,
    survey_name,
    survey_status,
    is_active
    -- add more columns from src_survey when needed
from 
    "airup_eu_dwh"."qualtrics"."survey"