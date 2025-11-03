
      
        
            
        delete from "n26"."credit_risk_playground"."bp_retail_provisions_stg_v5_pre_updated_v2"
    where (unique_key) in (
        select distinct unique_key
        from "bp_retail_provisions_stg_v5_pre_updated_v2__dbt_tmp162902716382" as DBT_INTERNAL_SOURCE
    )
    
    ;
    

    insert into "n26"."credit_risk_playground"."bp_retail_provisions_stg_v5_pre_updated_v2" ("user_id", "instrument_id", "legalentity", "is_ifrs", "reportdate", "calcdate", "script_calculation_date", "product_flag", "instrument_type", "instrument", "credit_creation_date", "start_in_arrears", "dpd", "insolvency", "dunning", "schufa_insolvency", "rep_plan_user", "is_default", "schufa_rating", "provider", "schufa_rating_date", "limit", "last_limit_eur", "last_limit_date", "tbil_exposures", "cc_exposures", "rp_2_exposures", "od_exposures", "porto_calibration_segment", "pit_calibration_segment", "internal_pd", "logit", "pd_pit_calibrated", "pd_pit_calibrated_down", "pd_pit_calibrated_neu", "pd_pit_calibrated_up", "pd_pit_calibrated_scen_weighted", "rating_class_pit_calibrated", "rating_class_pit_calibrated_down", "rating_class_pit_calibrated_neu", "rating_class_pit_calibrated_up", "rating_class_pit_calibrated_scen_weighted", "pd", "pd_down", "pd_neu", "pd_up", "pd_scen_weighted", "etl_updated", "unique_key", "country")
        (
            select "user_id", "instrument_id", "legalentity", "is_ifrs", "reportdate", "calcdate", "script_calculation_date", "product_flag", "instrument_type", "instrument", "credit_creation_date", "start_in_arrears", "dpd", "insolvency", "dunning", "schufa_insolvency", "rep_plan_user", "is_default", "schufa_rating", "provider", "schufa_rating_date", "limit", "last_limit_eur", "last_limit_date", "tbil_exposures", "cc_exposures", "rp_2_exposures", "od_exposures", "porto_calibration_segment", "pit_calibration_segment", "internal_pd", "logit", "pd_pit_calibrated", "pd_pit_calibrated_down", "pd_pit_calibrated_neu", "pd_pit_calibrated_up", "pd_pit_calibrated_scen_weighted", "rating_class_pit_calibrated", "rating_class_pit_calibrated_down", "rating_class_pit_calibrated_neu", "rating_class_pit_calibrated_up", "rating_class_pit_calibrated_scen_weighted", "pd", "pd_down", "pd_neu", "pd_up", "pd_scen_weighted", "etl_updated", "unique_key", "country"
            from "bp_retail_provisions_stg_v5_pre_updated_v2__dbt_tmp162902716382"
        )
  