
      
        
            
        delete from "n26"."credit_risk_playground"."bp_portfolio_customers_aud_m_v2"
    where (unique_key) in (
        select distinct unique_key
        from "bp_portfolio_customers_aud_m_v2__dbt_tmp154151782703" as DBT_INTERNAL_SOURCE
    )
    
    ;
    

    insert into "n26"."credit_risk_playground"."bp_portfolio_customers_aud_m_v2" ("user_id", "encoded_key", "reporting_date", "etl_updated", "unique_key")
        (
            select "user_id", "encoded_key", "reporting_date", "etl_updated", "unique_key"
            from "bp_portfolio_customers_aud_m_v2__dbt_tmp154151782703"
        )
  