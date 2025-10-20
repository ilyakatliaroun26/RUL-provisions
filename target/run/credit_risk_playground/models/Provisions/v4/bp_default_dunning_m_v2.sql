
      
        
            
        delete from "n26"."credit_risk_playground"."bp_default_dunning_m_v2"
    where (unique_key) in (
        select distinct unique_key
        from "bp_default_dunning_m_v2__dbt_tmp105246973897" as DBT_INTERNAL_SOURCE
    )
    
    ;
    

    insert into "n26"."credit_risk_playground"."bp_default_dunning_m_v2" ("user_id", "default_date", "reporting_date", "etl_updated", "unique_key")
        (
            select "user_id", "default_date", "reporting_date", "etl_updated", "unique_key"
            from "bp_default_dunning_m_v2__dbt_tmp105246973897"
        )
  