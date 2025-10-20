
      
        
            
        delete from "n26"."credit_risk_playground"."bp_social_benefits_v2_m_ik"
    where (unique_key) in (
        select distinct unique_key
        from "bp_social_benefits_v2_m_ik__dbt_tmp152511775045" as DBT_INTERNAL_SOURCE
    )
    
    ;
    

    insert into "n26"."credit_risk_playground"."bp_social_benefits_v2_m_ik" ("user_id", "latest_sb_date", "reporting_date", "etl_updated", "unique_key")
        (
            select "user_id", "latest_sb_date", "reporting_date", "etl_updated", "unique_key"
            from "bp_social_benefits_v2_m_ik__dbt_tmp152511775045"
        )
  