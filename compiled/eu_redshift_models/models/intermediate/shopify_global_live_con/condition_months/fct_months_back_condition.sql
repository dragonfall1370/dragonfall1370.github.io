 

select three_month::date as three_month_ago
from (select date_trunc('month', GETDATE()) - interval '3 month' as three_month)