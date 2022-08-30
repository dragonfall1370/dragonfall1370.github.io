

select 
    key as subquestion_key,
    question_id,
    survey_id,
    text,
    recode_value
from 
    "airup_eu_dwh"."qualtrics"."question_option"