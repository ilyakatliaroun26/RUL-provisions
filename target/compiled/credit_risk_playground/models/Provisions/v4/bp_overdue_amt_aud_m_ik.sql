

-- first drop it!

with 
overdraft_history as (
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
from dbt.bp_overdraft_users o
where   timeframe = 'day'
and end_time < getdate()
and end_time >= date_add('day',  -360, last_day(
                                                date_add('month'
                                                        , -1
                                                        , last_day(getdate())
                                                        )
                                                )
                         )
                         
order by end_time 
)

-- 3.2 Arrears data for Loans

, mmbr_loan_aud as (
select * 
, coalesce(lead(rev_timestamp - interval '0.000001 second', 1) over (partition by encoded_key order by rev_timestamp), '2100-01-01') as end_timestamp
from mmbr_n26_loan_account_aud 
)

, loans_dates AS ( 
SELECT lasa.encoded_key 
       , lasa.user_id 
       , d.end_time::date  AS date 
FROM dwh_cohort_dates d 
INNER JOIN ( SELECT m.encoded_key 
                    , mum.user_id 
                    , min(rev_timestamp::date)  AS disbursement_date 
                    , max(end_timestamp::date)  AS payback_date 
FROM mmbr_loan_aud m 
inner join dbt.mmbr_user_match mum on mum.mmbr_client_key = m.account_holder_key  and mum.is_current = true
WHERE principal_balance <> 0 
      OR interest_balance <> 0 
      OR interest_from_arrears_balance <> 0 
      OR fees_balance <> 0 
      OR penalty_balance <> 0
GROUP BY m.encoded_key , mum.user_id) lasa ON d.end_time::date BETWEEN lasa.disbursement_date AND lasa.payback_date 
WHERE d.date BETWEEN (last_day(
                            date_add('month'
                                    , -1
                                    , last_day(getdate())
                                    )
                            )+ interval'-360 days') AND last_day(
                                                                date_add('month'
                                                                        , -1
                                                                        , last_day(getdate())
                                                                        )
                                                                ) 
                                                                          
)

, end_ts as (
select *,
    coalesce(lead(rev_timestamp, 1) over (partition by encoded_key order by rev_timestamp), '2100-01-01') as end_timestamp
from mmbr_n26_repayment_aud mrsa
)

, overdue_loan as (
SELECT ld.user_id 
       , ld.encoded_key
       , ld.date as end_time 
       , SUM(CASE WHEN r.due_date::date <= ld.date AND r.state = 'LATE' THEN r.principal_due - r.principal_paid
                  ELSE 0 END) AS principal_overdue 
       , SUM(CASE WHEN r.due_date::date <= ld.date AND r.state = 'LATE' THEN r.interest_due - r.interest_paid 
                  ELSE 0 END) AS interest_overdue 
       , SUM(CASE WHEN (r.due_date AT TIME ZONE 'UTC')::date <= ld.date AND r.state = 'LATE' THEN r.fees_due - r.fees_paid 
                  ELSE 0 END) AS fees_overdue 
       , SUM(CASE WHEN (r.due_date AT TIME ZONE 'UTC')::date <= ld.date AND r.state = 'LATE' THEN r.penalty_due - r.penalty_paid 
                  ELSE 0 END) AS penalty_overdue 
FROM loans_dates ld 
LEFT JOIN end_ts r ON ld.encoded_key = r.parent_account_key 
                                   AND (ld.date BETWEEN r.rev_timestamp AND r.end_timestamp) 
-- where user_id = '107c3f96-9e5f-49e5-8994-04da6eb3dc41'
GROUP BY ld.user_id 
       , ld.encoded_key
       , ld.date 
ORDER BY  ld.user_id 
       , ld.encoded_key
       , ld.date 
)

, loan_history as (
select m.*
, mum.user_id 
, d.date as end_time
, (principal_overdue
   + interest_overdue 
   + fees_overdue
   + penalty_overdue) as overdue_amount
 , (principal_balance
    + interest_balance 
    + interest_from_arrears_balance 
    + fees_balance 
    + penalty_balance)  as exposures
from mmbr_loan_aud m
inner join dbt.mmbr_user_match mum on mum.mmbr_client_key = m.account_holder_key and mum.is_current = true
inner join dwh_cohort_dates d on  d.end_time::timestamp  between m.rev_timestamp  and m.end_timestamp
inner join dbt.mmbr_loan_product_mapping mp on m.loan_name = mp.loan_name and mp.product in ('consumer_credit'
                                                                                                , 'installment_loans'
                                                                                                , 'repayment_plans')
left join overdue_loan o on o.user_id = mum.user_id and o.encoded_key = m.encoded_key and  d.end_time::date = o.end_time::date 
where d.date >= date_add('day',  -360, last_day(
                                            date_add('month'
                                                    , -1
                                                    , last_day(getdate())
                                                    )
                                                ) 
                        )
and d.date < getdate()
and ( principal_balance <> 0 
            OR interest_balance <> 0 
            OR interest_from_arrears_balance <> 0 
            OR fees_balance <> 0 
            OR penalty_balance <> 0 )
)




 -- 3.3 Days past due calculation , all end_times are in "date" format so the reporting date in the end also should be transfered into "date"
 -- (based on https://www.eba.europa.eu/sites/default/documents/files/documents/10180/1597002/fe1db887-c6dc-4777-89c1-4f243584cafd/Final%20draft%20RTS%20on%20the%20materiality%20threshold%20for%20credit%20obligations%20%28EBA-RTS-2016-06%29.pdf?retry=1)

 , history_for_arrears as (
select distinct
    o.user_id 
    , o.end_time::date as end_time
    , case when o.overdue_amount < 0 then 0 else o.overdue_amount end overdue_amount
    , o.exposures
    from overdraft_history o 

    union all 

    select distinct
    c.user_id 
    , c.end_time::date as end_time
    , case when c.overdue_amount < 0 then 0 else c.overdue_amount end overdue_amount
    , c.exposures
    from loan_history c 
)


select 
user_id 
, end_time 
, getdate() as etl_updated
, coalesce(user_id, '') || coalesce(end_time::varchar, '') as unique_key
, sum(coalesce(overdue_amount,0)) as overdue_amount
, sum(coalesce(exposures,0)) as exposures
from history_for_arrears
group by 1,2,3,4
order by 1,2,3,4