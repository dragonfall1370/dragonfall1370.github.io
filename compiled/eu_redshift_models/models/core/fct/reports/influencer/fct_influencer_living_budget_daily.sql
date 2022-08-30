

with cte1 as (
select
	ilv.country,
	ilv.month,
	ilv.budget,
	coalesce(round(sum(die.total_costs), 2), 0) as total_costs
from
	"airup_eu_dwh"."living_budget"."influencer_living_budget_" ilv
left join "airup_eu_dwh"."influencer"."dim_influencer_enriched" die on
	ilv.month = date_trunc('month', die.date::timestamp with time zone)::date
	and ilv.country =
	case
		when die.country in ('Germany', 'Austria') then 'DACH'
		when die.country in ('Netherlands', 'Belgium') then 'BENELUX'
		else die.country
	end
group by
	ilv.country,
	ilv.month,
	ilv.budget
        ),
 date_generator as (
 with example as (
select
	('2020-01-01 00:00:00+01'::timestamp)::date as start_date,
	date_trunc('year', dateadd(year, 1, current_date))::date as end_date,
	datediff(day,
	('2020-01-01 00:00:00+01'::timestamp)::date,
	date_trunc('year', dateadd(year, 1, current_date))::date) as date_diff),
     number_series as
  (
select
	*
from
	reports.series_of_number
where
	gen_num between 0 and
       (
	select
		date_diff
	from
		example))
select
	*,
	(end_date::date - gen_num)::date as day_split
from
	example os
join number_series as s on
	1 = 1
 ),
cte2 as (
select
	cte1.country,
	cte1.month,
	cte1.total_costs / date_part(d, dateadd(day, -1, dateadd(month, 1, date_trunc('month', cte1.month)))) as cost_per_day,
	cte1.budget / date_part(d, dateadd(day, -1, dateadd(month, 1, date_trunc('month', cte1.month)))) as budget_per_day,
	date_part(d, dateadd(day, -1, dateadd(month, 1, date_trunc('month', cte1.month)))) as no_of_days_in_month,
	date_generator.day_split
from 
	date_generator join cte1 on
	cte1.month = date_trunc('month', date_generator.day_split)::date
        )
 select
	cte2.country,
	cte2.month,
	cte2.cost_per_day as total_cost_per_day,
	cte2.budget_per_day,
	cte2.no_of_days_in_month,
	cte2.day_split
from
	cte2