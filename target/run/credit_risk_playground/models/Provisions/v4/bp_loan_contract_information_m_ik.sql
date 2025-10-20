
      
        
            
        delete from "n26"."credit_risk_playground"."bp_loan_contract_information_m_ik"
    where (unique_key) in (
        select distinct unique_key
        from "bp_loan_contract_information_m_ik__dbt_tmp152051495258" as DBT_INTERNAL_SOURCE
    )
    
    ;
    

    insert into "n26"."credit_risk_playground"."bp_loan_contract_information_m_ik" ("encoded_key", "creation_date", "interest_rate_loan", "accrued_interest_loan", "loan_amount", "loan_name", "repayment_installments", "repayment_period_count", "principal_repayment_interval", "reporting_date", "etl_updated", "unique_key")
        (
            select "encoded_key", "creation_date", "interest_rate_loan", "accrued_interest_loan", "loan_amount", "loan_name", "repayment_installments", "repayment_period_count", "principal_repayment_interval", "reporting_date", "etl_updated", "unique_key"
            from "bp_loan_contract_information_m_ik__dbt_tmp152051495258"
        )
  