--- creator: Nham Dao
--- last modify: Nham Dao

select
	_file,
	shipment_no,
	"mode",
	pol,
	case
		when len(atd)>3 then to_date(atd , 'dd/mm/yyyy', false)
		else null
	end as atd,
	case
		when len(ata)>3 then to_date(ata , 'dd/mm/yyyy', false)
		else null
	end as ata,
	case when volume <> 'LCL'
	then cast(case when trim(' ' from volume) != '-' then cast (replace( replace(volume, '.', ''), ',', '.') as float ) else null end as float) 
	end as volume,
	unit,
	cast(case when trim(' ' from freight) != '-' then cast ( replace( replace(freight, '.', ''), ',', '.')as float ) else null end as float) as freight,
	--cast(case when trim(' ' from inland_charge) != '-' then cast ( replace( replace(inland_charge, '.', ''), ',', '.') as float ) else null end as float) as inland_charge,
	case when inland_charge not like '%#%' then
	cast(case when trim(' ' from inland_charge) != '-' then cast ( replace( replace(inland_charge, '.', ''), ',', '.') as float ) else null end as float)
	end as inland_charge,
	cast(case when trim(' ' from total_charges) != '-' then cast ( replace( replace(total_charges, '.', ''), ',', '.') as float ) else null end as float) as total_charges,
	invoice_no_,
	case when unit_price like '%,%' or unit_price like '%.%' then
	cast(case when trim(' ' from unit_price) != '-' then cast (trim(replace( replace((unit_price), '.', ''), ',', '.')) as float ) else null end as float)
	else null
	end as unit_price,
	forwarder
from
	"airup_eu_dwh"."logistics_inbound"."inbound_cost_manual"
where
	_file in ('/data/incoming/Inbound cost/EU-Inbound Cost.csv', '/data/incoming/Inbound cost/US-Inbound Cost.csv')