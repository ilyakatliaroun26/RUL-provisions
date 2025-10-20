

-- dbt run --full-refresh --select bp_hrc_m_v2 (first time)
-- dbt run --select bp_hrc_m_v2 (other runs)

with 
hrc_users as (
select * 
from dbt_pii.dash_hrc_history
where last_day(
            date_add('month'
                    , -1
                    , last_day(getdate())
                    )
            ) between nickel_event_start::timestamp and nickel_event_end::timestamp
and type = 'addHrc'
)

select * 
, last_day(
            date_add('month'
                    , -1
                    , last_day(getdate())
                    )
            ) as reporting_date
, getdate() as etl_updated
, coalesce(user_id, '') || coalesce(etl_updated::varchar, '') as unique_key
from hrc_users

where etl_updated > (select max(etl_updated) from "n26"."credit_risk_playground"."bp_hrc_m_v2")
