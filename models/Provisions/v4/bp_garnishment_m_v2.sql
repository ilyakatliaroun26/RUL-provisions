
{{
  config(
    materialized = "incremental",
    dist='user_id',
    sort='etl_updated',
    unique_key="unique_key",
    tags=["retail_provisions"]
    )
}}

-- dbt run --full-refresh --select bp_garnishment_m_v2 (first time)
-- dbt run --select bp_garnishment_m_v2 (other runs)

with 

garnishment as (
  select 
  customer_id as user_id
  , max(ng.created) as latest_garnishment_date
  from nd_garnishment ng
  left join nd_participant np  on ng.id = np.garnishment_id
  where np.type = 'RESPONDENT' --and ng.status = 'OPEN'

   and (
            (
            ng.status = 'CLOSED'
                and last_day(date_add('month', -1, last_day(getdate()))) between ng.created::timestamp and ng.updated::timestamp
            ) -- garnishment was active at the application, but now closed
            or 
            (
            ng.status = 'OPEN' and ng.created::timestamp <= last_day(date_add('month', -1, last_day(getdate())))
            )
        ) 
    group by 1 
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
from garnishment
{% if is_incremental() %}
where etl_updated > (select max(etl_updated) from {{this}})
{% endif %}