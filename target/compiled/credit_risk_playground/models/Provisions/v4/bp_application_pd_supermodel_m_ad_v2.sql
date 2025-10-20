

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
        row_number() over (partition by c.encoded_key, c.user_id order by ps.created desc) as row_num,
        pd,
        rating_class
    
    from "n26"."credit_risk_playground"."bp_portfolio_customers_aud_m_v2" c 
    inner join dbt_pii.credit_score_audit_log ps on c.user_id = ps.user_id 
    left join first_enabled_date f on f.encoded_key = c.encoded_key 
    left join mmbr_loan_account li on  li.encoded_key = c.encoded_key 
    where ps.model_name = 'PORTO'
          and ps.model_version = 'unified-1.0'
          and ps.score_type in ('behavioral')
           and c.reporting_date::date = last_day(
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
    where row_num = 1
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

where etl_updated > (select max(etl_updated) from "n26"."credit_risk_playground"."bp_application_pd_supermodel_m_ad_v2")
