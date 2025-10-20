

with 
hrc_users as (
select * 
from dbt_pii.dash_hrc_history
where '2025-06-11'::date between nickel_event_start::timestamp and nickel_event_end::timestamp
and type = 'addHrc'
)

select * 
, '2025-06-11'::date as reporting_date
, getdate() as etl_updated
, coalesce(user_id, '') || coalesce(etl_updated::varchar, '') as unique_key
from hrc_users

where etl_updated > (select max(etl_updated) from "n26"."credit_risk_playground"."bp_hrc_m_ik")
