
with stage AS (

  SELECT * FROM {{ ref('stg_salesforce__user_role') }}

),

reformatted AS (

  SELECT
    user_role_id,

-- renaming
    name                             AS user_role_name,
    parentroleid                     AS parent__user_role_id,
    rollupdescription                AS rollup_description,
    opportunityaccessforaccountowner AS opportunity_access_for_account_owner,
    caseaccessforaccountowner        AS case_access_for_account_owner,
    contactaccessforaccountowner     AS contact_access_for_account_owner,
    forecastuserid                   AS forecast__user_id,
    lastmodifiedbyid                 AS last_modified_by__user_id,
    portalaccountid                  AS portal__account_id,
    portaltype                       AS portal_type,
    portalrole                       AS portal_role,
    portalaccountownerid             AS portal_account_owner__user_id,

-- boolean conversions
    {{ convert_to_boolean('mayforecastmanagershare') }} AS may_forecast_manager_share,

-- date conversions
    {{ dbt_date.convert_timezone('lastmodifieddate', source_tz='America/Los_Angeles') }} AS date_last_modified,
    {{ dbt_date.convert_timezone('systemmodstamp', source_tz='America/Los_Angeles') }}   AS date_system_mod_stamp

  FROM stage

)

SELECT * FROM reformatted
