
select
	fss.*,
	sgm.group
from
	"airup_eu_dwh"."reports"."fct_shopify_sales" fss
left join "airup_eu_dwh"."reports"."sku_group_manual" sgm 
	on	fss.sku = sgm.sku