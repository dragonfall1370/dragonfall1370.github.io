--- creator: Nham Dao
--- last modify: Nham Dao
--- summary: clean data from purchase_order_line table. In case the PO was locked and unlocked, only take the latest information




with purchase_order_line as (select id,
	case
		when lower(date_planned) <> 'false' then date_planned::TIMESTAMP
		else null
	end as date_planned,write_date ,
	max(write_date) over (partition by (case when strpos(order_id, '"')>0 then
	split_part(replace(regexp_substr(order_id, '\"(.+)\"'), '"', ''), ' ', 1)
	else split_part(replace(regexp_substr(order_id, '\'(.+)\''), '\'', ''), ' ', 1) end)) as max_write_date,
	case when strpos(partner_id, '"')>0 then
	replace(regexp_substr(partner_id, '\"(.+)\"'), '"', '') 
	else replace(regexp_substr(partner_id, '\'(.+)\''), '\'', '') end as partner_id,
	case when strpos(partner_id, '"')>0 then
	split_part(replace(regexp_substr(partner_id, '\"(.+)\"'), '"', ''), ' ', 1)
	else split_part(replace(regexp_substr(partner_id, '\'(.+)\''), '\'', ''), ' ', 1) end as partner_id_number,
	date_order,
	case when strpos(product_id, '"')>0 then
	replace(regexp_substr(product_id, '\"(.+)\"'), '"', '') 
	else replace(regexp_substr(product_id, '\'(.+)\''), '\'', '')  end as product_id,
	case when strpos(order_id, '"')>0 then
	split_part(replace(regexp_substr(order_id, '\"(.+)\"'), '"', ''), ' ', 1)
	else split_part(replace(regexp_substr(order_id, '\'(.+)\''), '\'', ''), ' ', 1) end as order_id,
	case when strpos(currency_id, '"')>0 then
	replace(regexp_substr(currency_id, '\"(.+)\"'), '"', '')
	else replace(regexp_substr(currency_id, '\'(.+)\''), '\'', '') end as currency_id,
	qty_received,
	product_uom_qty as qty_ordered,
	price_total,
	price_unit,
	state
from
	"airup_eu_dwh"."odoo"."purchase_order_line"
	--odoo.purchase_order_line fpol
	where lower(state) != 'cancel'
	), 
	summary as --here we separate the data into 2 parts: state = draft and other cases. For state = draft, users are able to 
	-- revise/add/delete product id of 1 order. It's possible that the old and new information of product_id both exist in the database.
	-- therefore, we only take the latest write_date of the PO in "draft" state
	(select id, date_planned, partner_id, partner_id_number,date_order, product_id, order_id, currency_id,state,qty_ordered,
	qty_received,price_total,price_unit from purchase_order_line where lower(state) = 'draft' and write_date = max_write_date
	union 
	select id, date_planned, partner_id, partner_id_number,date_order, product_id, order_id, currency_id,state,qty_ordered,
	qty_received,price_total,price_unit from purchase_order_line where lower(state) != 'draft'
	)
select id,date_planned, partner_id, partner_id_number,date_order, product_id, order_id, currency_id,state, sum(qty_ordered) as qty_ordered,
	sum(qty_received) as qty_received, sum(price_total) as price_total, avg(price_unit) as price_unit 
from summary
group by 1,2,3,4,5,6,7,8,9