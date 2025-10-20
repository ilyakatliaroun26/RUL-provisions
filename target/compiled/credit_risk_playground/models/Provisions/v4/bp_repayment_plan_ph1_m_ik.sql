

with 
rep_plan as (
select distinct user_id 
from plutonium_repayment_plan
where (
        ( 
        status = 'ACTIVE' and created::date  <= '2025-06-11'::date
            )
or 
( status = 'CANCELLED' and '2025-06-11'::date
                                            between created::date  and  updated::date 
    )
)
)

select * 
, '2025-06-11'::date as reporting_date
, getdate() as etl_updated
, coalesce(user_id, '') || coalesce(etl_updated::varchar, '') as unique_key
from rep_plan

where etl_updated > (select max(etl_updated) from "n26"."credit_risk_playground"."bp_repayment_plan_ph1_m_ik")
