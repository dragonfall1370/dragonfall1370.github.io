


with
    prep as (
        select
            response.start_date,
            custom_dimensions.customer_id,
            custom_dimensions.shopLocale || '-' || custom_dimensions.order_id as order_number, -- combining shopLocale and order_id to get order_nummer shopify format
            response.response_text,
            response.response_text_2,
            CASE WHEN response.response_text_2 = '' then response.response_text else response.response_text_2 end as response_text_3
        from
            "airup_eu_dwh"."qualtrics"."dim_survey_responses" response
            left join "airup_eu_dwh"."qualtrics"."dim_survey_embedded_data_unpivoted" custom_dimensions using (response_id)
        where
            response.survey_id = 'SV_0pjBYJn6yg5MMFE' -- exit survey id
            and response.was_finished = 1
            and response.question_id = 'QID1'
    ),

    answer_mapping as (
        select
            prep.start_date,
            prep.customer_id,
            prep.order_number,
            prep.response_text,
            prep.response_text_2,
            prep.response_text_3,
            CASE 
                WHEN response_text = 'Influencer:in auf Social Media (z.B. Instagram, YouTube, TikTok, Switch)' then 'influencer'
                WHEN response_text = 'Werbung auf YouTube' then 'youtube'
                WHEN response_text = 'Werbung auf Instagram' then 'insta'
                WHEN response_text = 'Werbung auf Facebook' then 'facebook'
                WHEN response_text = 'Werbung auf Snapchat' then 'snapchat'
                WHEN response_text = 'Werbung auf TikTok' then 'tiktok'
                WHEN response_text = 'Freund:in, Bekannte:r oder Verwandte:r' then 'friend'
                WHEN response_text = 'TV' then 'tv'
                WHEN response_text = 'Radio oder Podcast' then 'radio'
                WHEN response_text = 'Zeitung oder Zeitschrift' then 'press'
                WHEN response_text = 'Im Laden (z.B. Rossmann, Müller)' then 'offline retail'
                WHEN response_text = 'Anders und zwar:' then 'other'
                else 'unmapped answer'
            end as response_mapped,
            TRUE as is_qualtrics_response 
        from
            prep
    )

select * from answer_mapping
where start_date >= '2022-08-01T12:45:00+00:00'