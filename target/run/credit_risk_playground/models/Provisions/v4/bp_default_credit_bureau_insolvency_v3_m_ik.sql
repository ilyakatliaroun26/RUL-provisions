
      
        
            
        delete from "n26"."credit_risk_playground"."bp_default_credit_bureau_insolvency_v3_m_ik"
    where (unique_key) in (
        select distinct unique_key
        from "bp_default_credit_bureau_insolvency_v3_m_ik__dbt_tmp151603251045" as DBT_INTERNAL_SOURCE
    )
    
    ;
    

    insert into "n26"."credit_risk_playground"."bp_default_credit_bureau_insolvency_v3_m_ik" ("user_id", "rev_timestamp", "reporting_date", "etl_updated", "unique_key")
        (
            select "user_id", "rev_timestamp", "reporting_date", "etl_updated", "unique_key"
            from "bp_default_credit_bureau_insolvency_v3_m_ik__dbt_tmp151603251045"
        )
  