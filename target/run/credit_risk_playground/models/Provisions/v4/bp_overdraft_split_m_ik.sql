
      
        
            
        delete from "n26"."credit_risk_playground"."bp_overdraft_split_m_ik"
    where (unique_key) in (
        select distinct unique_key
        from "bp_overdraft_split_m_ik__dbt_tmp142134340135" as DBT_INTERNAL_SOURCE
    )
    
    ;
    

    insert into "n26"."credit_risk_playground"."bp_overdraft_split_m_ik" ("user_id", "encoded_key", "balance", "max_date_neg_balance", "reporting_date", "od_type", "overdraft_lim", "etl_updated", "unique_key")
        (
            select "user_id", "encoded_key", "balance", "max_date_neg_balance", "reporting_date", "od_type", "overdraft_lim", "etl_updated", "unique_key"
            from "bp_overdraft_split_m_ik__dbt_tmp142134340135"
        )
  