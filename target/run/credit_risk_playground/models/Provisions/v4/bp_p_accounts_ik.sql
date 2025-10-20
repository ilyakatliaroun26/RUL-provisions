
  
    

  create  table
    "n26"."credit_risk_playground"."bp_p_accounts_ik__dbt_tmp"
    
    
    
  as (
    

with 
paccount as (
 select 
 user_id 
, created
, active 
, coalesce(allowance_cents,0) as allowance_cents
, lag(case when active = 1 then allowance_cents
           else 0 end) OVER (PARTITION by z.user_id order by created) as previous_allowance_cents
from "n26"."etl_reporting"."pa_paccount" p
inner join "n26"."dbt"."zrh_users" z on p.account_id = z.account_id
)
, paccount_ts as ( 
select 
user_id 
, active
, allowance_cents
, created as rev_timestamp 
, coalesce( lead(created) OVER (PARTITION BY user_id ORDER BY created) + interval'-0.000001 seconds' 
                  , '2100-01-01'::timestamp) as end_timestamp 
from paccount
where (previous_allowance_cents IS NULL OR previous_allowance_cents <> allowance_cents)
)



select p.* 
, getdate() as etl_updated
from  paccount_ts p
inner join "n26"."dbt"."zrh_users" z using(user_id)
where active = 1 and allowance_cents > 0 -- and a.closed_at is null
order by user_id, rev_timestamp
  );
  