{{
  config(
    materialized = "incremental",
    sort='etl_updated',
    unique_key="unique_key",
    tags=["retail_provisions"]
    )
}}

-- dbt run --full-refresh --select bp_portfolio_customers_aud_m_v2 (first time)
-- dbt run --models bp_portfolio_customers_aud_m_v2 (other runs)

    with 
    users as (
    select distinct
    user_id 
    , encoded_key
    , reporting_date
    from {{ ref('bp_portfolio_balance_aud_m_v2') }}
    where reporting_date::date = last_day(
                                          date_add('month'
                                                  , -1
                                                  , last_day(getdate())
                                                  )
                                                  ) 

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
    from {{ ref('bp_overdraft_limit_periods_v2') }}  f
    where  (f.amount_cents > 0) 
            and last_day(
                    date_add('month'
                            , -1
                            , last_day(getdate())
                            )
                            )  between f.rev_timestamp::timestamp and f.end_timestamp::timestamp
    )

    select distinct 
    user_id
    , encoded_key
    , reporting_date
    , getdate() as etl_updated
    , coalesce(encoded_key, '') || coalesce(etl_updated::varchar, '') as unique_key
    from users 
    {% if is_incremental() %}
    where etl_updated > (select max(etl_updated) from {{this}})
    {% endif %}   