

select 
    id as response_id,
    survey_id,
    start_date,
    end_date,
    recorded_date,
    progress,
    duration_in_seconds, 
    location_latitude,
    location_longitude,
    user_language,
    finished as was_finished
    -- add more columns from source when needed
from 
    "airup_eu_dwh"."qualtrics"."survey_response"