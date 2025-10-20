

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
            ng.status = 'CLOSE'
                and '2025-06-11'::date between ng.created::timestamp and ng.updated::timestamp
            ) -- garnishment was active at the application, but now closed
            or 
            (
            ng.status = 'OPEN' and ng.created::timestamp <= '2025-06-11'::date
            )
        ) 
    group by 1 
)

select * 
, '2025-06-11'::date as reporting_date
, getdate() as etl_updated
, coalesce(user_id, '') || coalesce(etl_updated::varchar, '') as unique_key
from garnishment

where etl_updated > (select max(etl_updated) from "n26"."credit_risk_playground"."bp_garnishment_m_ik")
