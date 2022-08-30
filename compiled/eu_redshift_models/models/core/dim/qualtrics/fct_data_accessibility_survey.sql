--- creator: Nham Dao
--- last modify: YuShih (add survey id 'SV_bxwGbkk4cFUTncW')
--- summary: data of accessibility survey

with question_id as
--get question id explicitly for the survey
  (
select
	q.id as question_id,
	q.survey_id,
	q.question_description
from
	"airup_eu_dwh"."qualtrics"."question" q
where
	(q.survey_id = ('SV_5ceXKurvBb1O5cW')
		and q.id in ('QID52',
                  'QID53',
                  'QID17',
                  'QID3',
                  'QID6',
                  'QID18',
                  'QID51',
                  'QID19',
                  'QID49'))
	or (q.survey_id = ('SV_9ZZnF7m5FLgjU46')
		and q.id in ('QID52',
                  'QID53',
                  'QID17',
                  'QID3',
                  'QID6'))
	or (q.survey_id = ('SV_bxwGbkk4cFUTncW')
		and q.id in ('QID52',
                  'QID53',
                  'QID17',
                  'QID3',
                  'QID6'))),			  
     question_sub_id as
--in case questions have subquestions, get subquestion information
  (
select
	qi.*,
	coalesce (sq."key",
	9999) as sub_question_key,
	sq.text as sub_question
from
	question_id qi
left join "airup_eu_dwh"."qualtrics"."sub_question" sq on
	qi.question_id = sq.question_id
	and qi.survey_id = sq.survey_id),
     response_id as
---get response id of the respondents for the survey
  (
select
	id as response_id,
	end_date as response_date,
	survey_id,
	(min(response_date) over (partition by survey_id))::date as survey_start_date
from
	"airup_eu_dwh"."qualtrics"."survey_response"
where
	survey_id in ('SV_5ceXKurvBb1O5cW', 'SV_9ZZnF7m5FLgjU46','SV_bxwGbkk4cFUTncW')
		and distribution_channel = 'anonymous'
		and "_fivetran_synced"::date >= '2022-02-16'),
     all_question_data as
--get the data for the data accessibility survey with respondents' answer
  (
select
	qsi.*,
	ri.response_id,
	ri.response_date::date,
	ri.survey_start_date,
	qr.value,
	qr."_fivetran_synced"
from
	"airup_eu_dwh"."qualtrics"."question_response" qr
inner join response_id ri on
	qr.response_id = ri.response_id
inner join question_sub_id qsi on
	qr.question_id = qsi.question_id
	and ri.survey_id  = qsi.survey_id
	and coalesce (qr.sub_question_key,
	9999) = qsi.sub_question_key),
     
	 
	 
	 splitter as
  (
select
	*
from
	"airup_eu_dwh"."reports"."series_of_number"
where
	gen_num between 1 and 12 )
 ,
     expanded_QID51 as
---question QID51 has multiple answers in the same cell, need to split it in mulitple rows - 1 row/1 answer
  (
select
	question_id,
	survey_id,
	question_description,
	sub_question_key,
	sub_question,
	response_id,
	response_date,
	survey_start_date,
	split_part(value, ',', s.gen_num) as value,
	"_fivetran_synced"
from
	(
	select
		*
	from
		all_question_data
	where
		question_id = 'QID51') as ts
join splitter as s on
	1 = 1
where
	split_part(value, ',', s.gen_num) <> '' ),
     summary_data as
  (
select
	*
from
	all_question_data
where
	question_id <> 'QID51'
union
select
	*
from
	expanded_QID51),
     response_in_text as
     --get the response answer explicitly in text
  (
select
	*
from
	qualtrics.question_option
where
	survey_id in ('SV_5ceXKurvBb1O5cW', 'SV_9ZZnF7m5FLgjU46','SV_bxwGbkk4cFUTncW')
		and question_id in ('QID52',
                         'QID53',
                         'QID17',
                         'QID3',
                         'QID6',
                         'QID18',
                         'QID51',
                         'QID19',
                         'QID49') ),
	data_accessibility_survey as
	(					 
select
	sd.*,
	dense_rank() over (
	order by survey_start_date desc) as time_rank,
	rit.text as response_in_text
from
	summary_data sd
left join response_in_text as rit 
	on sd.question_id = rit.question_id
	and sd.survey_id = rit.survey_id
	and sd.value = rit.key ),
	team_division as
	--get the team division from question id QID52 for the suvery
	(
select response_id , response_in_text
from data_accessibility_survey
where question_id = 'QID52'
  )
select das.*, td.response_in_text as division
from data_accessibility_survey das
left join team_division td
	on das.response_id = td.response_id