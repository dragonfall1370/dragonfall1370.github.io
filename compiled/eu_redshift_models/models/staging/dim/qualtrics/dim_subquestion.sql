

select 
    key as subquestion_key,
    question_id,
    survey_id,
    text as answer
    -- add more columns from source when needed
from 
    "airup_eu_dwh"."qualtrics"."sub_question"