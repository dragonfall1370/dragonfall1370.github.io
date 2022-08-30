

select 
    id as question_id,
    survey_id,
    question_text,
    question_description,
    question_type,
    selector,
    next_choice_id,
    next_answer_id,
    validation_setting_force_response as forced_response
    -- add more columns from source when needed
from 
    "airup_eu_dwh"."qualtrics"."question"