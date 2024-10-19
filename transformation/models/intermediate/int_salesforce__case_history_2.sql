

with stage AS (

  SELECT * FROM {{ ref('stg_salesforce__case_history_2') }}

),

reformatted AS (

  SELECT
    case_history_id,

-- renaming
    caseid           AS case_id,
    ownerid          AS case_history_owner__user_id,
    status           AS case_history_status,
    previousupdate   AS previous_update,
    lastmodifiedbyid AS last_modified_by__user_id,

-- boolean conversions
    {{ convert_to_boolean('isdeleted') }} AS is_deleted,

-- date conversions
    {{ dbt_date.convert_timezone('lastmodifieddate', source_tz='America/Los_Angeles') }} AS date_last_modified,
    {{ dbt_date.convert_timezone('systemmodstamp', source_tz='America/Los_Angeles') }}   AS date_system_mod_stamp

  FROM stage

)

SELECT * FROM reformatted
