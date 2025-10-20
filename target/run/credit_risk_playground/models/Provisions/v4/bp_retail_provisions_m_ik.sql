
      
        
            
        delete from "n26"."credit_risk_playground"."bp_retail_provisions_m_ik"
    where (unique_key) in (
        select distinct unique_key
        from "bp_retail_provisions_m_ik__dbt_tmp154409566008" as DBT_INTERNAL_SOURCE
    )
    
    ;
    

    insert into "n26"."credit_risk_playground"."bp_retail_provisions_m_ik" ("user_id", "instrument_id", "country", "legalentity", "is_ifrs", "reportdate", "calcdate", "script_calculation_date", "product_flag", "instrument_type", "instrument", "credit_creation_date", "start_in_arrears", "dpd", "insolvency", "dunning", "schufa_insolvency", "rep_plan_user", "is_default", "schufa_rating", "provider", "schufa_rating_date", "limit", "last_limit_eur", "last_limit_date", "tbil_exposures", "cc_exposures", "rp_2_exposures", "od_exposures", "logit", "pd_pit", "pd_od", "pd_pit_calibrated", "rating_class_pit_calibrated", "pd", "currentrating", "currentrating_date", "utilization_pct", "usage", "ccf", "off_balance_exposure", "offbalance_ead", "onbalance_ead", "isoffbalance", "lgd", "overdraft_interest_accrued", "interest_rate_loan", "accrued_interest_loan", "first_repayment", "last_repayment", "loan_amount", "loan_name", "repayment_installments", "repayment_period_count", "principal_repayment_interval", "maturitydate", "maturity", "initialpd_date", "initialpd", "initialpd_lifetime", "pd_lifetime", "stage", "stage_description", "sllp", "sllp_od", "sllp_cc", "sllp_tbil", "sllp_rp_2", "gllp_od", "gllp_rp_2", "gllp_cc", "gllp_tbil", "gllp", "ecl_on", "ecl_off", "etl_updated", "unique_key")
        (
            select "user_id", "instrument_id", "country", "legalentity", "is_ifrs", "reportdate", "calcdate", "script_calculation_date", "product_flag", "instrument_type", "instrument", "credit_creation_date", "start_in_arrears", "dpd", "insolvency", "dunning", "schufa_insolvency", "rep_plan_user", "is_default", "schufa_rating", "provider", "schufa_rating_date", "limit", "last_limit_eur", "last_limit_date", "tbil_exposures", "cc_exposures", "rp_2_exposures", "od_exposures", "logit", "pd_pit", "pd_od", "pd_pit_calibrated", "rating_class_pit_calibrated", "pd", "currentrating", "currentrating_date", "utilization_pct", "usage", "ccf", "off_balance_exposure", "offbalance_ead", "onbalance_ead", "isoffbalance", "lgd", "overdraft_interest_accrued", "interest_rate_loan", "accrued_interest_loan", "first_repayment", "last_repayment", "loan_amount", "loan_name", "repayment_installments", "repayment_period_count", "principal_repayment_interval", "maturitydate", "maturity", "initialpd_date", "initialpd", "initialpd_lifetime", "pd_lifetime", "stage", "stage_description", "sllp", "sllp_od", "sllp_cc", "sllp_tbil", "sllp_rp_2", "gllp_od", "gllp_rp_2", "gllp_cc", "gllp_tbil", "gllp", "ecl_on", "ecl_off", "etl_updated", "unique_key"
            from "bp_retail_provisions_m_ik__dbt_tmp154409566008"
        )
  