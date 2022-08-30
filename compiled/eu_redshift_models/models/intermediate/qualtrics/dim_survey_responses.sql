---###################################
---##### Developed and tested for exit_survey; 
---#####test you survey before making any analysis
---###################################




with
    -- answers/values are in an array which needs to be unested into rows (see CTEs below)
    nested_table as (
        select
        survey_response.response_id,
        survey_response.survey_id,
        survey_response.start_date,
        survey_response.end_date,
        survey_response.was_finished,
        question_response.question_id,
        question_response.subquestion_key,
        question_response.question_value,
        question_response.subquestion_text,
        survey_response.response_id || survey_response.survey_id || question_response.question_id || coalesce(question_response.subquestion_key, 999) as unique_key
    from
        "airup_eu_dwh"."qualtrics"."dim_survey_response"  survey_response
        left join "airup_eu_dwh"."qualtrics"."dim_question_response" question_response
            on survey_response.response_id = question_response.response_id
    --where survey_id = 'SV_0pjBYJn6yg5MMFE' 
    group by 1,2,3,4,5,6,7,8,9,10
    ),

-- ###################
-- unesting array
-- ##################

    -- unests the row and counts the lenght of the array
    nested_rows AS (
        SELECT
        unique_key AS id
        , NULLIF(TRIM(question_value),'') AS nested_value
        , REGEXP_COUNT(NULLIF(TRIM(question_value),''), ',') +1 AS length
        FROM nested_table
        WHERE NULLIF(TRIM(question_value),'') IS NOT NULL
        	AND subquestion_text is null
    ),
    -- pulls a series of numbers starting with 0
    sequence as (
        SELECT
        gen_num as i
        FROM "airup_eu_dwh"."reports"."series_of_number"
        WHERE  i < (SELECT MAX(length) FROM nested_rows)
    ),

    unnested as (
        SELECT
            nested_rows.id AS unique_key,
            TRIM(SPLIT_PART(nested_rows.nested_value, ',', sequence.i + 1)) AS question_value_unnested
        FROM nested_rows
    INNER JOIN sequence ON sequence.i <= nested_rows.length),

-- ###################
-- finished unnesting array
-- ##################

    -- final unnesting
    unnested_response as (
        select
            nested_table.unique_key,
            response_id,
            survey_id,
            start_date,
            end_date,
            was_finished,
            question_id,
            subquestion_key,
            subquestion_text,
            question_value,
            question_value_unnested as question_value_unnested
        from
            nested_table
                left join unnested
                    on nested_table.unique_key = unnested.unique_key
        			and question_value_unnested != ''

    ),

    -- mapping all response values to questions and subquestions
    -- populating answers (1-3) for different question types
    prep as (
        select
            response.response_id,
            response.question_id,
            response.survey_id,
            response.start_date,
            response.end_date,
            response.was_finished,
            question.question_description,
            coalesce(subquestion.answer, 'n/a') as subquestion_text,
            response.question_value_unnested as response_value,
            options.text as response_text,
            response_other.question_value as response_text_2
        from
            unnested_response response
            left join "airup_eu_dwh"."qualtrics"."dim_question" question
                on response.question_id = question.question_id
                and response.survey_id = question.survey_id
            left join "airup_eu_dwh"."qualtrics"."dim_subquestion" subquestion
                on response.question_id = subquestion.question_id
                and response.survey_id = subquestion.survey_id
                and response.subquestion_key = subquestion.subquestion_key
            left join "airup_eu_dwh"."qualtrics"."dim_question_option" options
                on response.question_id = options.question_id
                and response.survey_id = options.survey_id
                and response.question_value = options.recode_value
            left join unnested_response response_other -- self join bringing in the custom answers as a answer
                on response.unique_key = response_other.unique_key
                and response.question_value = response_other.question_value_unnested
                and response_other.subquestion_text is not null
        where response.subquestion_text is null --do not remove; removes rows with custom answers
    )


select * from prep