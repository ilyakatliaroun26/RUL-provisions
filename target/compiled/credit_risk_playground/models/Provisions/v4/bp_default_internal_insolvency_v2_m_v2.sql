

-- dbt run --full-refresh --select bp_default_internal_insolvency_v2_m_v2 (first time)
-- dbt run --select bp_default_internal_insolvency_v2_m_v2 (other runs)

with 

insolvency_curr as (
select  
    np.customer_id as user_id
    , max(ni.origin_date::date) as internal_insolvency_date
from "n26"."public"."neodymium_insolvency"  ni
left join "n26"."public"."nd_participant" np  on ni.id = np.garnishment_id
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

where etl_updated > (select max(etl_updated) from "n26"."credit_risk_playground"."bp_default_internal_insolvency_v2_m_v2")
