

with
    -- creating dimensional table for customer who opted-in for email
	opt_to_email as
		(
			select
				lower(person.email) as email,
				max(case when custom_consent like '%email%' then 1 else 0 end) as opt_to_email
			from
				klaviyo_global.dim_person person
			group by 1
			)

select
    *
from 
    opt_to_email