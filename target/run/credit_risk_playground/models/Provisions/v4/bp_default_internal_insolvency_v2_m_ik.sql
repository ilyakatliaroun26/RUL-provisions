
      
        
            
        delete from "n26"."credit_risk_playground"."bp_default_internal_insolvency_v2_m_ik"
    where (unique_key) in (
        select distinct unique_key
        from "bp_default_internal_insolvency_v2_m_ik__dbt_tmp151123474327" as DBT_INTERNAL_SOURCE
    )
    
    ;
    

    insert into "n26"."credit_risk_playground"."bp_default_internal_insolvency_v2_m_ik" ("user_id", "internal_insolvency_date", "reporting_date", "etl_updated", "unique_key")
        (
            select "user_id", "internal_insolvency_date", "reporting_date", "etl_updated", "unique_key"
            from "bp_default_internal_insolvency_v2_m_ik__dbt_tmp151123474327"
        )
  