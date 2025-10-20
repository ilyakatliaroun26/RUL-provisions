
      
        
            
        delete from "n26"."credit_risk_playground"."bp_credit_bureau_rating_m_ik"
    where (unique_key) in (
        select distinct unique_key
        from "bp_credit_bureau_rating_m_ik__dbt_tmp150753860687" as DBT_INTERNAL_SOURCE
    )
    
    ;
    

    insert into "n26"."credit_risk_playground"."bp_credit_bureau_rating_m_ik" ("user_id", "rating", "provider", "rev_timestamp", "reporting_date", "etl_updated", "unique_key")
        (
            select "user_id", "rating", "provider", "rev_timestamp", "reporting_date", "etl_updated", "unique_key"
            from "bp_credit_bureau_rating_m_ik__dbt_tmp150753860687"
        )
  