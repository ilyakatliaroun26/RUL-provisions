


with  
mmbr_loan_aud as (
select * 
, coalesce(lead(rev_timestamp - interval '0.000001 second', 1) over (partition by encoded_key order by rev_timestamp), '2100-01-01') as end_timestamp
from mmbr_n26_loan_account_aud 
)
 
, loans_additional_data as (
select 
ml.encoded_key
, ml.creation_date
, m.interest_rate as interest_rate_loan
, m.accrued_interest as accrued_interest_loan
, m.loan_amount
, m.loan_name
, m.repayment_installments
, m.repayment_period_count
, m.principal_repayment_interval
from mmbr_loan_account ml 
left join mmbr_loan_aud m on m.encoded_key = ml.encoded_key
-- reporting date to the last minute
where date_add('minute'
        , -1
        , date_add('day'
            , 1
            , '2025-06-11'::date
            
                ) 
           ) between rev_timestamp and end_timestamp
            )


select * 
, '2025-06-11'::date as reporting_date 
, getdate() as etl_updated
, coalesce(encoded_key, '') || coalesce(etl_updated::varchar, '') as unique_key
from loans_additional_data

where etl_updated > (select max(etl_updated) from "n26"."credit_risk_playground"."bp_loan_contract_information_m_ik")
