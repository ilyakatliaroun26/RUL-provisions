
{{
  config(
    materialized = "incremental",
    dist='user_id',
    sort='etl_updated',
    unique_key="unique_key",
    tags=["retail_provisions"]
    )
}}

-- dbt run --full-refresh --select bp_repayment_plan_ph1_m_v2 (first time)
-- dbt run --select bp_repayment_plan_ph1_m_v2 (other runs)

with 
rep_plan as (
select distinct user_id 
from plutonium_repayment_plan
where (
        ( 
        status = 'ACTIVE' and created::date  <= last_day(
                                                    date_add('month'
                                                            , -1
                                                            , last_day(getdate()
                                                                        )
                                                            ) 
                                                        )
            )
or 
( status = 'CANCELLED' and last_day(
                                    date_add('month'
                                            , -1
                                            , last_day(getdate()
                                                        )
                                                )
                                    )
                                            between created::date  and  updated::date 
    )
)
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
from rep_plan
{% if is_incremental() %}
where etl_updated > (select max(etl_updated) from {{this}})
{% endif %}
