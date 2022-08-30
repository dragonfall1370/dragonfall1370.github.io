--- creator: Nham Dao
--- last modify: Nham Dao
--- summary: clean data from stock_move_line table. In case the PO was locked and unlocked, only take the latest information



select
	origin,
	reference,
	case when strpos(product_id, '"')>0 then
	replace(regexp_substr(product_id , '\"(.+)\"'), '"', '')
	else replace(regexp_substr(product_id , '\'(.+)\''), '\'', '') end as product_id,
	state,
	sum(product_qty) as product_qty,
	sum(qty_done) as qty_done
from
	"airup_eu_dwh"."odoo"."stock_move_line"
	where state not in ('cancel', 'false')
group by
	1,
	2,
	3,
	4