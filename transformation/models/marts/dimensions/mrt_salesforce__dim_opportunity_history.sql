{{ config(alias = 'dim_salesforce_opportunity_history') }}

WITH base_opportunity_history AS (

  SELECT * FROM {{ ref('int_salesforce__opportunity_history') }}

),

opportunity_history AS (

  SELECT
    opportunity_id,
    opportunity_created_date_for_insert,
    opportunity_from_forecast_category,
    opportunity_forecast_category,
    opportunity_prev_forecast_update,
    from_opportunity_stage_name,
    prev_opportunity_stage_update,
    date_opportunity_record_valid_from,
    date_opportunity_record_valid_through

  FROM base_opportunity_history

)

SELECT * FROM opportunity_history
