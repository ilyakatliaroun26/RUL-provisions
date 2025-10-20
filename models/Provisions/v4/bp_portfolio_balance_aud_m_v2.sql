{{
  config(
    materialized = "incremental",
    dist='encoded_key',
    sort='etl_updated',
    unique_key="unique_key",
    tags=["retail_provisions"]
    )
}}

-- dbt run --full-refresh --select bp_portfolio_balance_aud_m (first time)
-- dbt run --models bp_portfolio_balance_aud_m (other runs)

with

overdraft_gl_accounts as (
        select
            cl.user_id
            , s.encoded_key
            , last_day(
                    date_add('month'
                            , -1
                            , last_day(getdate())
                            )
                    ) as reporting_date
            , 'Overdraft' as  product
            , sum(
                case
                    when glje."type" = 'CREDIT' then glje.amount * (-1)
                    when glje."type" = 'DEBIT' then glje.amount
                end
            ) as balance
        from {{ source('public', 'mmbr_gl_account') }} as gla
        inner join {{ source('public', 'mmbr_gl_journal_entry') }} as glje
            on gla.encoded_key = glje.glaccount_encodedkey_oid
        inner join {{ source('public', 'mmbr_savings_account') }} s on glje.account_key = s.encoded_key 
                                                                        and s.account_type = 'CURRENT_ACCOUNT'
        inner join {{ source('dbt', 'mmbr_user_match') }} cl 
            on cl.encoded_key = s.encoded_key and cl.is_current = true
        where gla.gl_code = '104.00.00' and glje.entry_date::date <= last_day(
                                                                            date_add('month'
                                                                                    , -1
                                                                                    , last_day(getdate())
                                                                                    )
                                                                            )
        group by 1,2,3,4
        having sum(
                case
                    when glje."type" = 'CREDIT' then glje.amount * (-1)
                    when glje."type" = 'DEBIT' then glje.amount
                end
            ) <> 0
        )


, loan_balance as (
        select
            cl.user_id
            , s.encoded_key
            , last_day(
                    date_add('month'
                            , -1
                            , last_day(getdate())
                            )
                            ) as reporting_date
            , case when gl_code = '104.00.05' then 'Consumer Credit'
                    when gl_code = '104' then 'Consumer Credit'
                    when gl_code = '104.00.06' then 'Installment Loans'
                    when gl_code = '104.00.07' then 'Repayment Plane (Phase 2)' 
                    end as product
            , sum(
                case
                    when glje."type" = 'CREDIT' then glje.amount * (-1)
                    when glje."type" = 'DEBIT' then glje.amount
                end
            ) as balance
        from {{ source('public', 'mmbr_gl_account') }}  as gla
        inner join {{ source('public', 'mmbr_gl_journal_entry') }} as glje
            on gla.encoded_key = glje.glaccount_encodedkey_oid
        left join {{ source('public', 'mmbr_loan_account') }} s on glje.account_key = s.encoded_key
        left join {{ source('dbt', 'mmbr_user_match') }} cl
            on cl.mmbr_client_key = s.account_holder_key and cl.is_current = true
        where gla.gl_code in ('104.00.05', '104', '104.00.06', '104.00.07') and glje.entry_date::date <= last_day(
                                                                                                                date_add('month'
                                                                                                                        , -1
                                                                                                                        , last_day(getdate())
                                                                                                                        )
                                                                                                                )
            group by 1, 2, 3, 4
        having sum(
                case
                    when glje."type" = 'CREDIT' then glje.amount * (-1)
                    when glje."type" = 'DEBIT' then glje.amount
                end
            ) <> 0
)

, total as (
select distinct 
        user_id 
        , encoded_key 
        , last_day(
                    date_add('month'
                            , -1
                            , last_day(getdate())
                            )
                            ) as reporting_date
        , product
        , balance 
        , getdate() as etl_updated
from overdraft_gl_accounts

union all 

select distinct 
        user_id 
        , encoded_key 
        , reporting_date
        , product
        , balance 
        , getdate() as etl_updated
from loan_balance
)

select distinct
user_id 
, encoded_key 
, reporting_date
, product
, balance 
, coalesce(encoded_key, '') || coalesce(etl_updated::varchar, '') as unique_key
, etl_updated
from total 
{% if is_incremental() %}
where etl_updated > (select max(etl_updated) from {{this}})
{% endif %}


