
{{
    config(
    materialized = "table",
    tags=["retail_provisions"]
    )
}}

with 


max_merkmal_date as (

SELECT csr.user_id, MAX(csr.audit_rev_timestamp) as rev_timestamp
FROM private.californium_credit_score_record_request_audit_log csr
inner join credit_risk_playground.bp_portfolio_customers_aud_m_v2 as portfolio ON portfolio.user_id = csr.user_id
where --csr.audit_rev_timestamp <= LAST_DAY(date_add('month', -1, GETDATE()))

csr.audit_rev_timestamp::timestamp <= date_add('minute'
                                                        , -1
                                                        , date_add('day'
                                                            , 1
                                                            , last_day(
                                                                    date_add('month'
                                                                        , -1
                                                                        , last_day(getdate())
                                                                        )
                                                                    )
                                                            
                                                                ) 
                                                           )

GROUP BY csr.user_id

)


select * 
from max_merkmal_date