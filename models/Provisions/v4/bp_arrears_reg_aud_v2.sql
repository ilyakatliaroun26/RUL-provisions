
{{
  config(
    materialized = "table",
    tags=["retail_provisions"]
    )
}}

-- first drop it!

with 
delcy_days as (
select * 
from {{ ref('bp_overdue_amt_aud_m_v2') }} 
where case when coalesce(exposures::numeric,0) = 0 and coalesce(overdue_amount::numeric,0) = 0 then 0
           when coalesce(overdue_amount::numeric,0) > 0 
                and coalesce(overdue_amount::numeric,0) >= 100 
                and coalesce(overdue_amount::numeric,0) > coalesce(exposures::numeric,0)/100  then 1
             else -1  end = 1 
order by end_time
)

, data as (
    select * 
           , datediff(day
                      , end_time
                      , lead(end_time,1) over (PARTITION BY user_id ORDER BY end_time asc)) as day_diff
    from delcy_days
    order by end_time
)

, first_date as (select * 
       , case when lag(day_diff)  over (PARTITION BY user_id ORDER BY end_time asc) <> 1 then end_time 
              when lag(day_diff)  over (PARTITION BY user_id ORDER BY end_time asc) is null then end_time 
              else null end as date_first
       , case when day_diff  <> 1 then end_time 
              when day_diff is null then end_time 
              else null end as date_end
from data
order by end_time)

, dpd as (
select * 
, max(date_first) ignore NULLS over(PARTITION BY user_id ORDER BY end_time asc
                                                                            rows between unbounded preceding and current row) as start_in_arrears
, min(date_end) ignore NULLS over(PARTITION BY user_id ORDER BY end_time asc
                                                                            rows between current row  and unbounded following) as end_in_arrears
from first_date
)


select distinct user_id
       , start_in_arrears
       , end_in_arrears
from dpd
order by user_id, start_in_arrears