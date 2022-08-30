--created by: Nham Dao

--This view show the price unit of each product id per supplier



with exchange_rate AS
   (select conversion_rate_eur as exchanged_rate_usd_eur,round(1/conversion_rate_eur, 4) as exchanged_rate_eur_usd from "airup_eu_dwh"."odoo_currency"."dim_global_currency_rates"
where creation_date  = (select max(creation_date) from "airup_eu_dwh"."odoo_currency"."dim_global_currency_rates")
and currency_abbreviation = 'USD'),
product_product_clean as
  (
select
	distinct case when strpos(categ_id, '"')>0 then 
    replace(regexp_substr(categ_id, '\"(.+)\"'), '"', '')
    else
    replace(regexp_substr(categ_id, '\'(.+)\''), '\'', '') end as product_cat,
    case when strpos(product_variant_id, '"')>0 then 
    replace(regexp_substr(product_variant_id, '\"(.+)\"'), '"', '')
    else
	replace(regexp_substr(product_variant_id, '\'(.+)\''), '\'', '') end as product_id
from
	"airup_eu_dwh"."odoo"."product_product"
	where lower(default_code) <> 'false'),
supplier_info as (select
	case when replace(regexp_substr(product_tmpl_id , '\'(.+)\''), '\'', '') like '%0%' then replace(regexp_substr(product_tmpl_id , '\'(.+)\''), '\'', '')
	else replace(regexp_substr(product_id , '\'(.+)\''), '\'', '') end as product_id,
	replace(regexp_substr(currency_id , '\'(.+)\''), '\'', '') as currency,
	case
		when lower(date_start) <> 'false' then date(date_start)
		else '2020-06-01'
	end as date_start,
	case
		when lower(date_end) <> 'false' then date(date_end)
		else date(dateadd(month,12,current_date))
	end as date_end,
	replace(regexp_substr(name , '\'(.+)\''), '\'', '') as "name",
        price
from
	"airup_eu_dwh"."odoo"."product_supplierinfo"), 
---for the date start which is not at beginning of the month or date end which is not end of the month, we need to modify the date_start and date_end
supplier_info_enriched as 
(select supplier_info.*,product_product_clean.product_cat,
date(date_trunc('month', date_start)) as new_date_start, 
date(case when date_end <> LAST_DAY( date_end) 
then date_add('month',-1,LAST_DAY( date_end)) 
else  LAST_DAY( date_end)  end) as new_date_end
from supplier_info 
left join product_product_clean
on supplier_info.product_id =product_product_clean.product_id ),
summary as (select *,round(months_between(new_date_end,new_date_start)) as month_diff, row_number() over(partition by product_id, "name" order by date_start is not null, date_start) as row_index  
from supplier_info_enriched, exchange_rate)
select *, date(dateadd(month,gen_num,date_start) ) as new_date from summary
left join
(select * from "airup_eu_dwh"."reports"."series_of_number"
where gen_num <= (select max(month_diff) from summary)) t1
on 1 = 1
where gen_num < month_diff