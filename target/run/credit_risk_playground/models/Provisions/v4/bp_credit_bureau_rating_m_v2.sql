
      
        
            
        delete from "n26"."credit_risk_playground"."bp_credit_bureau_rating_m_v2"
    where (unique_key) in (
        select distinct unique_key
        from "bp_credit_bureau_rating_m_v2__dbt_tmp105037168141" as DBT_INTERNAL_SOURCE
    )
    
    ;
    

    insert into "n26"."credit_risk_playground"."bp_credit_bureau_rating_m_v2" ("user_id", "rating", "provider", "rev_timestamp", "reporting_date", "etl_updated", "unique_key")
        (
            select "user_id", "rating", "provider", "rev_timestamp", "reporting_date", "etl_updated", "unique_key"
            from "bp_credit_bureau_rating_m_v2__dbt_tmp105037168141"
        )
  