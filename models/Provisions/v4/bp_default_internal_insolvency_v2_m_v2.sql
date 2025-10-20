
{{
  config(
    materialized = "incremental",
    dist='user_id',
    sort='etl_updated',
    unique_key="unique_key",
    tags=["retail_provisions"]
    )
}}

-- dbt run --full-refresh --select bp_default_internal_insolvency_v2_m_v2 (first time)
-- dbt run --select bp_default_internal_insolvency_v2_m_v2 (other runs)

with 

insolvency_curr as (
select  
    np.customer_id as user_id
    , max(ni.origin_date::date) as internal_insolvency_date
from {{ source('public', 'neodymium_insolvency') }}  ni
left join {{ source('public', 'nd_participant') }} np  on ni.id = np.garnishment_id
where np.type = 'RESPONDENT' 
   and (
            (
            ni.status = 'CLOSE'
                and last_day(date_add('month', -1, last_day(getdate()))) between ni.created::timestamp and ni.updated::timestamp
            ) -- garnishment was active at the application, but now closed
            or 
            (
            ni.status = 'OPEN' and ni.created::timestamp <= last_day(date_add('month', -1, last_day(getdate())))
            )
        ) 
group by np.customer_id 
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
from insolvency_curr
{% if is_incremental() %}
where etl_updated > (select max(etl_updated) from {{this}})
{% endif %}
