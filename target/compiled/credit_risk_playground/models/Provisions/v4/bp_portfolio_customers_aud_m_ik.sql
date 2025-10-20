
    with 
    users as (
    select distinct
    user_id 
    , encoded_key
    , reporting_date
    from "n26"."credit_risk_playground"."bp_portfolio_balance_aud_m_ik"
    where reporting_date::date = '2025-06-11'::date 

    union all 

    select 
    f.user_id
    , f.encoded_key
    , last_day(
              date_add('month'
                      , -1
                      , last_day(getdate())
                      )
                      ) as reporting_date
    from "n26"."credit_risk_playground"."bp_overdraft_limit_periods_ik"  f
    where  (f.amount_cents > 0) 
            and '2025-06-11'::date  between f.rev_timestamp::timestamp and f.end_timestamp::timestamp
    )

    select distinct 
    user_id
    , encoded_key
    , reporting_date
    , getdate() as etl_updated
    , coalesce(encoded_key, '') || coalesce(etl_updated::varchar, '') as unique_key
    from users 
    
    where etl_updated > (select max(etl_updated) from "n26"."credit_risk_playground"."bp_portfolio_customers_aud_m_ik")
    