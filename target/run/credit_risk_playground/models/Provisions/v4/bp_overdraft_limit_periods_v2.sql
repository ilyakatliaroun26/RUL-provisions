
  
    

  create  table
    "n26"."credit_risk_playground"."bp_overdraft_limit_periods_v2__dbt_tmp"
    
    
    
  as (
    

with 

od_users as (
select distinct user_id 
from "n26"."public"."pu_overdraft_history"
where status = 'ENABLED'

UNION

select distinct z.id as user_id
from "n26"."public"."ddb_overdraft_settings_aud" 
inner join etl_reporting.cmd_users z using(user_created)
where enabled = 1 

)

, ddb_os as (
select 
z.id as user_id
, d.user_created
, rev_timestamp
, case when enabled = 1 then max_amount_cents 
       else 0 end as amount_cents 
, lag(case when enabled = 1 then max_amount_cents
           else 0 end) OVER (PARTITION by user_created order by rev_timestamp) as previous_amount_cents 
from  "n26"."public"."ddb_overdraft_settings_aud" d 
inner join "n26"."etl_reporting"."cmd_users" z using(user_created)
, (select min(created) as mpd 
    from "n26"."public"."pu_overdraft_history") min_pu_date
where d.rev_timestamp < mpd -- coalesce(mp.mpud , mpd)
)

, plutonium as (
 select 
 user_id 
, user_created 
, created as rev_timestamp
, case when status ='ENABLED' then amount_cents 
       else 0 end as amount_cents 
, lag(case when status = 'ENABLED' then amount_cents 
           else 0 end) OVER (PARTITION by user_created order by created) as previous_amount_cents 
from "n26"."public"."pu_overdraft_history" 
)

, limits_history as (
select distinct
a.* 
from (
select * 
from ddb_os
union all 
select * 
from plutonium ) a
order by a.user_id, a.user_created, a.rev_timestamp
)

, limits_periods as ( 
select 
user_created 
, user_id 
, rev_timestamp 
, coalesce( lead(rev_timestamp) OVER (PARTITION BY user_created ORDER BY rev_timestamp) + interval'-0.000001 seconds' 
                  , '2100-01-01'::timestamp) as end_timestamp 
, amount_cents 
from plutonium
where (previous_amount_cents IS NULL OR previous_amount_cents <> amount_cents) ) 

select 
f.* 
, s.encoded_key
, getdate() as etl_updated
from limits_periods f
inner join od_users o on o.user_id = f.user_id
inner join "n26"."dbt"."mmbr_user_match" cl on cl.user_id = f.user_id  and cl.is_current = true
inner join "n26"."public"."mmbr_savings_account" s on s.encoded_key = cl.encoded_key 
                                                                and s.account_type = 'CURRENT_ACCOUNT'
  );
  