

with stage AS (

  SELECT * FROM {{ ref('stg_salesforce__opportunity_history') }}

),

add_record_valid_from AS (

  SELECT
    *,
    COALESCE(
      LAG(validthroughdate) OVER (PARTITION BY opportunityid ORDER BY validthroughdate),
      '1900-01-01' :: TIMESTAMP
    ) AS record_valid_from

  FROM stage

),

reformatted AS (

  SELECT
    opportunity_history_id,

-- renaming
    opportunityid              AS opportunity_id,
    createdbyid                AS opportunity_created_by__user_id,
    createddateforinsert       AS opportunity_created_date_for_insert,
    stagename                  AS opportunity_stage_name,
    amount                     AS opportunity_amount,
    expectedrevenue            AS opportunity_expected_revenue,
    probability                AS opportunity_probability,
    fromforecastcategory       AS opportunity_from_forecast_category,
    forecastcategory           AS opportunity_forecast_category,
    prevforecastupdate         AS opportunity_prev_forecast_update,
    fromopportunitystagename   AS from_opportunity_stage_name,
    prevopportunitystageupdate AS prev_opportunity_stage_update,

-- boolean conversions
    {{ convert_to_boolean('isdeleted') }} AS is_opportunity_deleted,

-- date conversions
    {{ dbt_date.convert_timezone('createddate', source_tz='America/Los_Angeles') }}       AS date_opportunity_created,
    {{ dbt_date.convert_timezone('closedate', source_tz='America/Los_Angeles') }}         AS date_opportunity_close,
    {{ dbt_date.convert_timezone('record_valid_from', source_tz='America/Los_Angeles') }} AS date_opportunity_record_valid_from,
    {{ dbt_date.convert_timezone('validthroughdate', source_tz='America/Los_Angeles') }}  AS date_opportunity_record_valid_through,
    {{ dbt_date.convert_timezone('systemmodstamp', source_tz='America/Los_Angeles') }}    AS date_opportunity_system_mod_stamp

  FROM add_record_valid_from

)

SELECT * FROM reformatted
