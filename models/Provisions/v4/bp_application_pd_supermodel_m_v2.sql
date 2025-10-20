{{
  config(
    materialized = "incremental",
    sort='etl_updated',
    unique_key="unique_key",
    tags=["retail_provisions"]
    )
}}

-- dbt run --full-refresh --select bp_application_pd_supermodel_m_v2 (first time)
-- dbt run --select bp_application_pd_supermodel_m_v2 (other runs)

with 

first_enabled_date as (
select 
user_id 
, encoded_key
, min(rev_timestamp) as first_creation_date
, max(rev_timestamp) as last_creation_date
from {{ ref('bp_overdraft_limit_periods_v2') }}
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
from {{ref('bp_portfolio_customers_aud_m_v2')}}
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
        row_number() over (partition by c.encoded_key, c.user_id order by ps.created desc) as row_num,
        pd,
        rating_class,
        ps.score_type
    
    from portfolio c 
    inner join dbt_pii.credit_score_audit_log ps on c.user_id = ps.user_id 
    inner join mmbr_loan_account li on  li.encoded_key = c.encoded_key 
    inner join dbt.mmbr_loan_product_mapping mp on li.loan_name = mp.loan_name and mp.product in ('consumer_credit')
    where ps.model_name = 'PORTO'
          and ps.model_version = 'unified-1.0'
          and ps.score_type in ('application_CC') -- IMPORTANT! Application scores time is THE SAME , make 
          and ps.created::date <= li.creation_date::date
          and ps.created::date >= '2025-02-25'::date
          and li.creation_date is not null
     )
     
 , pd_app_tbil as (
      select
        c.user_id,
        c.encoded_key,
        ps.created::date as calculated_at, 
        row_number() over (partition by c.encoded_key, c.user_id order by ps.created desc) as row_num,
        pd,
        rating_class,
        ps.score_type
    
    from portfolio c 
    inner join dbt_pii.credit_score_audit_log ps on c.user_id = ps.user_id 
    inner join mmbr_loan_account li on  li.encoded_key = c.encoded_key 
    inner join dbt.mmbr_loan_product_mapping mp on li.loan_name = mp.loan_name and mp.product in ('installment_loans')
    where ps.model_name = 'PORTO'
          and ps.model_version = 'unified-1.0'
          and ps.score_type in ('application_TBIL') -- IMPORTANT! Application scores time is THE SAME , make 
          and ps.created::date <= li.creation_date::date
          and ps.created::date >= '2025-02-25'::date
          and li.creation_date is not null
 )
 
 , pd_app_od as (
      select
        c.user_id,
        c.encoded_key,
        ps.created::date as calculated_at, 
        row_number() over (partition by c.encoded_key, c.user_id order by ps.created desc) as row_num,
        pd,
        rating_class,
        ps.score_type
    
    from portfolio c 
    inner join dbt_pii.credit_score_audit_log ps on c.user_id = ps.user_id 
    inner join first_enabled_date f on f.encoded_key = c.encoded_key 
    where ps.model_name = 'PORTO'
          and ps.model_version = 'unified-1.0'
          and ps.score_type in ('application_OD') 
          and ps.created::date <= f.first_creation_date::date
          and ps.created::date >= '2025-02-25'::date
          and f.first_creation_date is not null
 )


, porto_app as (
    select
        user_id
        , encoded_key
        , pd  as pd_2
        , calculated_at
        , rating_class 
    from pd_app_cc 
    where row_num = 1

    union all 

    select
        user_id
        , encoded_key
        , pd  as pd_2
        , calculated_at
        , rating_class 
    from pd_app_tbil
    where row_num = 1

    union all 

    select
        user_id
        , encoded_key
        , pd  as pd_2
        , calculated_at
        , rating_class 
    from pd_app_od
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
{% if is_incremental() %}
where etl_updated > (select max(etl_updated) from {{this}})
{% endif %}



