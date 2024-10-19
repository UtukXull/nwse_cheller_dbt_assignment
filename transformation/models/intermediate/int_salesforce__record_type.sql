

with stage AS (

  SELECT * FROM {{ ref('stg_salesforce__record_type') }}

),

reformatted AS (

  SELECT
    record_type_id,

-- renaming
    name              AS record_type_name,
    modulenamespace   AS module_namespace,
    description       AS record_type_description,
    businessprocessid AS business_process_id,
    sobjecttype       AS object_type,
    createdbyid       AS created_by__user_id,
    lastmodifiedbyid  AS last_modified_by__user_id,

-- boolean conversions
    {{ convert_to_boolean('isdeleted') }} AS is_deleted,
    {{ convert_to_boolean('isactive') }}  AS is_active,

-- date conversions
    {{ dbt_date.convert_timezone('createddate', source_tz='America/Los_Angeles') }}      AS date_created,
    {{ dbt_date.convert_timezone('lastmodifieddate', source_tz='America/Los_Angeles') }} AS date_last_modified,
    {{ dbt_date.convert_timezone('systemmodstamp', source_tz='America/Los_Angeles') }}   AS date_system_mod_stamp

  FROM stage

)

SELECT * FROM reformatted
