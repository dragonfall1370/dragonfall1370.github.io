

select 
    response_id,
    question_id,
    sub_question_key as subquestion_key,
    sub_question_text as subquestion_text,
    value as question_value
from 
    "airup_eu_dwh"."qualtrics"."question_response"