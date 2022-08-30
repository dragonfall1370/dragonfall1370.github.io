--Authors: YuShih Hsieh
---Last Modified by: YuShih Hsieh

---###################################################################################################################
        -- this view contains the information on the US quality inspection survey from Qualitrics
        -- the survey id = SV_5zJ09D2YnwLqAMm
---###################################################################################################################



-- pulling all the data in narrow format
with prep as (   
  select distinct response_id,
        question_id,
        start_date,
        end_date,
        question_description,
        subquestion_text,
        response_value,
        response_text
    from qualtrics.dim_survey_responses
    where survey_id = 'SV_5zJ09D2YnwLqAMm'
    and was_finished = '1'
    and response_value is not null
), prep2 as (
    select distinct response_id 
    from prep
),
-- unpivotting the needed colums via self joins
  unpivotting as (
    select prep2.response_id,
        proudct_type.response_text as proudct_type,
        product_name.response_text as product_name,
        batch_code.response_value as batch_code,
        minor_defects.response_text as physical_damage_to_packaging_minor_defects,
        major_defects.response_text as physical_damage_to_packaging_major_defects
    from prep2
    left join prep proudct_type
    on prep2.response_id = proudct_type.response_id 
        and proudct_type.question_description = 'Select both the type of product you are inspecting and the size of the lot in this shipment.'
    left join prep batch_code
    on prep2.response_id = batch_code.response_id 
        and batch_code.question_description = 'What is the batch code of the product being inspected?'
    left join prep product_name 
    on prep2.response_id = product_name.response_id 
        and product_name.question_description = 'What is the item number / article number of the product being inspected?'
    left join prep minor_defects
    on prep2.response_id = minor_defects.response_id 
        and minor_defects.question_description = 'Physical Damage to Packaging' 
        and minor_defects.subquestion_text = 'Minor Defects'
    left join prep major_defects
    on prep2.response_id = major_defects.response_id 
        and major_defects.question_description = 'Physical Damage to Packaging' 
        and major_defects.subquestion_text = 'Major Defects'
)
  select *
  from unpivotting