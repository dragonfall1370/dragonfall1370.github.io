---Authors: Nham Dao
---Last Modified by: Nham Dao

---###################################################################################################################

        --- This view contains data to regarding the customer satisfactory survey
---###################################################################################################################

 

WITH survey_score AS (
         SELECT sr.survey_id,
            sr.agent_id,
            sr.group_id,
            sr.ticket_id,
            sr.feedback,
            sr.created_at,
            sr.updated_at,
            sr.contact_id,
            sr._fivetran_synced,
            sr.id,
            srl.value
           FROM "airup_eu_dwh"."freshdesk"."satisfaction_rating" sr
             LEFT JOIN "airup_eu_dwh"."freshdesk"."satisfaction_rating_value" srl ON sr.id = srl.satisfaction_rating_id
          WHERE sr.survey_id = '48000069796'::bigint AND srl.survey_question_id::text = 'default_question'::text
        )
 SELECT date(survey_score.created_at) AS created_at,
    survey_score.value,
    survey_score.ticket_id,
        CASE
            WHEN survey_score.value::integer < 100 THEN 'negative'::text
            WHEN survey_score.value::integer = 100 THEN 'neutral'::text
            WHEN survey_score.value::integer > 100 THEN 'positive'::text
            ELSE 'undefined'::text
        END AS csat
   FROM survey_score