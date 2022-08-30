


select '€ 1 = '|| res_currency.symbol|| ' '|| left(round(res_currency_rate.rate::double precision, 2),4) as exchange_rate



from "airup_eu_dwh"."odoo_currency"."res_currency" res_currency



join "airup_eu_dwh"."odoo_currency"."res_currency_rate" res_currency_rate



on res_currency.id = res_currency_rate.currency_id



where res_currency_rate.create_date = (SELECT MAX(create_date) from "airup_eu_dwh"."odoo_currency"."res_currency_rate")