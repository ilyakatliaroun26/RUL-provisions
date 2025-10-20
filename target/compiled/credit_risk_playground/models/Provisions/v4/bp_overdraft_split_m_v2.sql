

-- dbt run --full-refresh --select bp_overdraft_split_m_v2 (first time)
-- dbt run --select bp_overdraft_split_m_v2 (other runs)

with 

max_date_limit as  (
select  s.user_id 
, max(end_timestamp) as max_date_neg_balance
from "n26"."credit_risk_playground"."bp_overdraft_limit_periods_v2" s 
where s.amount_cents > 0 and s.rev_timestamp::date <= last_day(
                                                        date_add('month'
                                                               , -1
                                                               , last_day(getdate())
                                                               )
                                                        )
group by s.user_id 
)

, overdraft_history as (
select distinct
o.user_id
, o.end_time
, o.max_amount_cents
, o.od_enabled_flag
, datediff('day'
           , o.end_time
           , lead(o.end_time,1) over(partition by o.user_id order by o.end_time)) as dd 
, coalesce(o.outstanding_balance_eur::numeric,0) - coalesce(o.max_amount_cents::numeric/100,0) as overdue_amount
, coalesce(o.outstanding_balance_eur::numeric,0) as exposures
from "n26"."dbt"."bp_overdraft_users" o
where   timeframe = 'day'
and end_time <= last_day(
                     date_add('month'
                            , -1
                            , last_day(getdate())
                            )
                     )
order by end_time 
)

-- 4.2 Flag if there was a top-up after the last limit became 0 (also captures users with utilisation 0)

, top_up as ( 
select distinct
s.user_id
from overdraft_history  s
inner join max_date_limit b on b.user_id = s.user_id and s.end_time between max_date_neg_balance and last_day(
                                                                                                         date_add('month'
                                                                                                                , -1
                                                                                                                , last_day(getdate())
                                                                                                                )
                                                                                                         )::date
where (coalesce(s.exposures,0) = 0 or s.dd > 1) 
)

-- 4.3 Last limit for overdraft users (plutonium)

, last_limit as (
select s.user_id
, s.amount_cents as last_limit 
from "n26"."credit_risk_playground"."bp_overdraft_limit_periods_v2"  s
inner join max_date_limit b on b.user_id = s.user_id and s.end_timestamp = max_date_neg_balance
where s.amount_cents > 0 and s.rev_timestamp::date <= last_day(
                                                        date_add('month'
                                                               , -1
                                                               , last_day(getdate())
                                                               )
                                                        )
)

-- Balances 

, mambu_savings_accounts as (
select * 
from "n26"."credit_risk_playground"."bp_portfolio_balance_aud_m_v2" b
where b.reporting_date = last_day(
                            date_add('month'
                                   , -1
                                   , last_day(getdate())
                                   )
                            )
       and b.product = 'Overdraft'
)

-- 4.4 Overdraft portfolio with Arranged/Unarranged split


select 
m.user_id 
, m.encoded_key
, m.balance 
, n.max_date_neg_balance
, last_day(
       date_add('month'
              , -1
              , last_day(getdate())
              )
       ) as reporting_date
, case when (n.user_id is not null and t.user_id is null) or n.max_date_neg_balance::date = last_day(
                                                                                                  date_add('month'
                                                                                                                , -1
                                                                                                                , last_day(getdate())
                                                                                                                )
                                                                                                  )::date then 'Arranged Overdraft' else 'Unarranged Overdraft' end as OD_type
, case when (n.user_id is not null and t.user_id is null) or n.max_date_neg_balance::date = last_day(
                                                                                                  date_add('month'
                                                                                                                , -1
                                                                                                                , last_day(getdate())
                                                                                                                )
                                                                                                  )::date
                                                                            then la.last_limit/100
       else 0 end overdraft_lim
, getdate() as etl_updated
, coalesce(m.user_id, '') || coalesce(etl_updated::varchar, '') as unique_key
from mambu_savings_accounts m 
left join top_up t on t.user_id = m.user_id 
left join max_date_limit n on n.user_id = m.user_id
left join last_limit la on la.user_id = m.user_id

where etl_updated > (select max(etl_updated) from "n26"."credit_risk_playground"."bp_overdraft_split_m_v2")
