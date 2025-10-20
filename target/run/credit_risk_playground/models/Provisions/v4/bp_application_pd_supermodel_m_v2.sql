
      
        
            
        delete from "n26"."credit_risk_playground"."bp_application_pd_supermodel_m_v2"
    where (unique_key) in (
        select distinct unique_key
        from "bp_application_pd_supermodel_m_v2__dbt_tmp175906900846" as DBT_INTERNAL_SOURCE
    )
    
    ;
    

    insert into "n26"."credit_risk_playground"."bp_application_pd_supermodel_m_v2" ("user_id", "encoded_key", "pd_2", "calculated_at", "rating_class", "reporting_date", "etl_updated", "unique_key")
        (
            select "user_id", "encoded_key", "pd_2", "calculated_at", "rating_class", "reporting_date", "etl_updated", "unique_key"
            from "bp_application_pd_supermodel_m_v2__dbt_tmp175906900846"
        )
  