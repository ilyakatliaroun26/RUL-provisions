
      
  
    

  create  table
    "n26"."credit_risk_playground"."bp_application_pd_supermodel_m_v2_impact"
    
    
    
      compound sortkey(etl_updated)
  as (
    

-- dbt run --full-refresh --select bp_application_pd_supermodel_m_v2 (first time)
-- dbt run --select bp_application_pd_supermodel_m_v2 (other runs)

with 

first_enabled_date as (
select 
user_id 
, encoded_key
, min(rev_timestamp) as first_creation_date
, max(rev_timestamp) as last_creation_date
from "n26"."credit_risk_playground"."bp_overdraft_limit_periods_v2"
where rev_timestamp::date <= last_day(
                            date_add('month'
                                    , -1
                                    , last_day(getdate())
                                    )
                            ) and amount_cents > 0 
group by 
user_id 
, encoded_key

)

, portfolio as (
select * 
from "n26"."credit_risk_playground"."bp_portfolio_customers_aud_m_v2"
where reporting_date::date = last_day(
                                        date_add('month'
                                                , -1
                                                , last_day(getdate())
                                                )
                                        )
)

, pd_app_cc as (

    select
        c.user_id,
        c.encoded_key,
        ps.created::date as calculated_at, 
        ps.pd,
        ps.rating_class

    from portfolio c 
    inner join credit_risk_playground.bp_pd_application_provisions_aug_31 ps on c.user_id = ps.user_id and c.encoded_key = ps.instrument_id
    inner join mmbr_loan_account li on  li.encoded_key = c.encoded_key 
    inner join dbt.mmbr_loan_product_mapping mp on li.loan_name = mp.loan_name and mp.product in ('consumer_credit')
    where ps.product_flag = 'CC'
     )
     
 , pd_app_tbil as (
      select
        c.user_id,
        c.encoded_key,
        ps.created::date as calculated_at, 
        ps.pd,
        ps.rating_class
    
    from portfolio c 
    inner join credit_risk_playground.bp_pd_application_provisions_aug_31 ps on c.user_id = ps.user_id and c.encoded_key = ps.instrument_id
    inner join mmbr_loan_account li on  li.encoded_key = c.encoded_key 
    inner join dbt.mmbr_loan_product_mapping mp on li.loan_name = mp.loan_name and mp.product in ('installment_loans')
    where ps.product_flag = 'TBIL'
 )
 
 , pd_app_od as (
      select
        c.user_id,
        c.encoded_key,
        ps.created::date as calculated_at, 
        ps.pd,
        ps.rating_class
    
    from portfolio c 
    inner join credit_risk_playground.bp_pd_application_provisions_aug_31 ps on c.user_id = ps.user_id
    inner join first_enabled_date f on f.encoded_key = c.encoded_key
    where ps.product_flag = 'Arranged Overdraft'
 )


, porto_app as (
    select
        user_id
        , encoded_key
        , pd  as pd_2
        , calculated_at
        , rating_class 
    from pd_app_cc 

    union all 

    select
        user_id
        , encoded_key
        , pd  as pd_2
        , calculated_at
        , rating_class 
    from pd_app_tbil

    union all 

    select
        user_id
        , encoded_key
        , pd  as pd_2
        , calculated_at
        , rating_class 
    from pd_app_od
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
from porto_app

  );
  
  