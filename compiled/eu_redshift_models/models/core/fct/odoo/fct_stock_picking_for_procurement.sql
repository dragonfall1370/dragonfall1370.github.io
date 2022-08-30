--- creator: Nham Dao
--- last modify: Nham Dao
--- summary: clean procurement data from stock_picking table


with stock_picking as 
(
select
	"name",
	state,
	date_done,
	create_date,
	scheduled_date,
	product_id,
	origin
from
	"airup_eu_dwh"."odoo"."stock_picking" sp
where
	(name like 'GEOAP/IN/%'
	or name like 'DCUK1/IN/%'
	or name like 'DEDE1/IN/%'
	or name like 'DCFR1/IN/%')
	and state != 'cancel' 
	group by 1,2,3,4,5,6,7)
,
splitter as
(
select
	*
from
	"airup_eu_dwh"."reports"."series_of_number"
where
	gen_num between 1 and (
	select
		max(REGEXP_COUNT(origin, ',') + 1)
	from
		stock_picking sp)
)
--select * from splitter;
  ,
expanded_input as
(
select
	"name",
	state,
	case
		when lower(date_done) <> 'false' then date_done::timestamp
		else null
	end as date_done,
	create_date,
	scheduled_date,
	case when strpos(product_id, '"')>0 then
	replace(regexp_substr(product_id , '\"(.+)\"'), '"', '')
	else replace(regexp_substr(product_id , '\'(.+)\''), '\'', '')  end as product_id,
	split_part(origin, ',', s.gen_num) as origin
from
	stock_picking as tp
join splitter as s on
	1 = 1
where
	split_part(origin, ',', s.gen_num) <> ''
) ,
summary as (
select
	expanded_input.name,
	case
		when regexp_instr(expanded_input.origin,
		'PO') = 0
			and expanded_input.origin is not null
			and expanded_input.origin <> 'false' then concat('PO-', expanded_input.origin)
			else expanded_input.origin
		end as origin,
		expanded_input.state,
		expanded_input.product_id,
		expanded_input.date_done,
		expanded_input.scheduled_date,
		expanded_input.create_date
	from
		expanded_input
	where
		expanded_input.state <> 'cancel'),

stock_move_line_clean as
---need to include data from stock_move_line since product_id column in stock_picking table is not correct
(
select
	*
from
	"airup_eu_dwh"."odoo"."fct_stock_move_line"
where
	reference like 'GEOAP/IN/%'
	or reference like 'DCUK1/IN/%'
	or reference like 'DEDE1/IN/%'
	or reference like 'DCFR1/IN/%'),

data_with_correct_product_id as 
(
select
	"name",
	replace(spc.origin, 'ß', 0) as origin,
	spc.state,
	sml.product_id,
	sml.product_qty,
	sml.qty_done,
	spc.date_done,
	spc.scheduled_date,
	spc.create_date
from
	summary spc
left join stock_move_line_clean sml 
on
	spc."name" = sml.reference)

select
	*
from
	data_with_correct_product_id