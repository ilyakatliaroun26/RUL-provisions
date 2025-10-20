
      
        
            
        delete from "n26"."credit_risk_playground"."bp_garnishment_m_ik"
    where (unique_key) in (
        select distinct unique_key
        from "bp_garnishment_m_ik__dbt_tmp152822039128" as DBT_INTERNAL_SOURCE
    )
    
    ;
    

    insert into "n26"."credit_risk_playground"."bp_garnishment_m_ik" ("user_id", "latest_garnishment_date", "reporting_date", "etl_updated", "unique_key")
        (
            select "user_id", "latest_garnishment_date", "reporting_date", "etl_updated", "unique_key"
            from "bp_garnishment_m_ik__dbt_tmp152822039128"
        )
  