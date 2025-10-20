
      
        
            
        delete from "n26"."credit_risk_playground"."bp_application_pd_supermodel_m_ik"
    where (unique_key) in (
        select distinct unique_key
        from "bp_application_pd_supermodel_m_ik__dbt_tmp152945431095" as DBT_INTERNAL_SOURCE
    )
    
    ;
    

    insert into "n26"."credit_risk_playground"."bp_application_pd_supermodel_m_ik" ("user_id", "encoded_key", "pd_2", "calculated_at", "rating_class", "reporting_date", "etl_updated", "unique_key")
        (
            select "user_id", "encoded_key", "pd_2", "calculated_at", "rating_class", "reporting_date", "etl_updated", "unique_key"
            from "bp_application_pd_supermodel_m_ik__dbt_tmp152945431095"
        )
  