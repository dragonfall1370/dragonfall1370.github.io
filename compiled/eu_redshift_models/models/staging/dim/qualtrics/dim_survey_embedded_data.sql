

select 
    response_id,
    REGEXP_REPLACE(key, '''') as key, -- cleaning the column of apostrophs 
    value
from 
    "airup_eu_dwh"."qualtrics"."survey_embedded_data"