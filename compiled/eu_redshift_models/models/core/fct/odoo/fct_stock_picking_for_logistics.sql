

select
	id, name, scheduled_date, origin, sale_teams, carrier_tracking_ref, carrier_id, state, create_date, write_date, date_done, location_dest_id, 
	product_id, backorder_id, backorder_ids, glue_timestamp, _fivetran_synced, _fivetran_batch, _fivetran_index, picking_eta, picking_type_id, location_id
from
	"airup_eu_dwh"."odoo"."stock_picking" sp
where
	sale_teams like 'D2C%'
    or sale_teams = 'Goodwill'
    or sale_teams = 'Influencer'