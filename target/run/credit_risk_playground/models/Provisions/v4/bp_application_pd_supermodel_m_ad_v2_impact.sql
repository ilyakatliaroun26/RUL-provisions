
      
  
    

  create  table
    "n26"."credit_risk_playground"."bp_application_pd_supermodel_m_ad_v2_impact"
    
    
    
      compound sortkey(etl_updated)
  as (
    

-- dbt run --full-refresh --select bp_application_pd_supermodel_m_ad_v2 (first time)
-- dbt run --select bp_application_pd_supermodel_m_ad_v2 (other runs)

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


, pd_app as (
    select
        c.user_id,
        c.encoded_key,
        ps.created::date as calculated_at, 
        ps.pd,
        ps.rating_class
    
    from "n26"."credit_risk_playground"."bp_portfolio_customers_aud_m_v2" c 
    inner join credit_risk_playground.bp_pd_application_provisions_aug_31 ps on c.user_id = ps.user_id 
    left join first_enabled_date f on f.encoded_key = c.encoded_key 
    left join mmbr_loan_account li on  li.encoded_key = c.encoded_key 
           where c.reporting_date::date = last_day(
                                        date_add('month'
                                                , -1
                                                , last_day(getdate())
                                                )
                                        )
           and ps.created::date <= coalesce(f.first_creation_date::date, li.creation_date::date)
          and coalesce(f.first_creation_date::date, li.creation_date::date) is not null
)


, porto_app as (
    select
        user_id
        , encoded_key
        , pd  as pd_2
        , calculated_at
        , rating_class 
    from pd_app
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
  
  