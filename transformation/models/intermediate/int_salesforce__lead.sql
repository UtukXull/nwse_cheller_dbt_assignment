

with stage AS (

  SELECT * FROM {{ ref('stg_salesforce__lead') }}

),

reformatted AS (

  SELECT

--keys
    lead_id,
    ownerid                AS lead_owner__user_id,
    convertedaccountid     AS converted__account_id,
    convertedcontactid     AS converted__contact_id,
    convertedopportunityid AS converted__opportunity_id,
    createdbyid            AS created_by__user_id,
    lastmodifiedbyid       AS last_modified_by__user_id,
    jigsawcontactid        AS jigsaw__contact_id,
    masterrecordid         AS master_record_id,
    dandbcompanyid         AS dandb_company_id,
    individualid           AS individual_id,

--direct fields
    title,
    company,
    street,
    city,
    state,
    country,
    latitude,
    longitude,
    phone,
    fax,
    email,
    website,
    industry,
    rating,
    jigsaw,
    pronouns,

-- renamed fields
    salutation           AS salutation,
    firstname            AS first_name,
    lastname             AS last_name,
    postalcode           AS postal_code,
    geocodeaccuracy      AS geocode_accuracy,
    mobilephone          AS mobile_phone,
    description          AS lead_description,
    leadsource           AS lead_source,
    status               AS lead_status,
    annualrevenue        AS annual_revenue,
    numberofemployees    AS number_of_employees,
    cleanstatus          AS clean_status,
    companydunsnumber    AS company_duns_number,
    emailbouncedreason   AS email_bounced_reason,
    genderidentity       AS gender_identity,
    siccode__c           AS sic_code,
    productinterest__c   AS product_interest,
    currentgenerators__c AS current_generators,
    numberoflocations__c AS number_of_locations,

-- boolean conversions
    {{ convert_to_boolean('isdeleted') }}               AS is_deleted,
    {{ convert_to_boolean('hasoptedoutofemail') }}      AS has_opted_out_of_email,
    {{ convert_to_boolean('isconverted') }}             AS is_converted,
    {{ convert_to_boolean('isunreadbyowner') }}         AS is_unread_by_owner,
    {{ convert_to_boolean('donotcall') }}               AS has_do_not_call,
    {{ convert_to_boolean('hasoptedoutoffax') }}        AS has_opted_out_of_fax,
    {{ convert_to_boolean('primary__c', 'yes', 'no') }} AS is_primary,

-- date conversions
    {{ dbt_date.convert_timezone('converteddate', source_tz='America/Los_Angeles') }}    AS date_converted,
    {{ dbt_date.convert_timezone('createddate', source_tz='America/Los_Angeles') }}      AS date_created,
    {{ dbt_date.convert_timezone('lastmodifieddate', source_tz='America/Los_Angeles') }} AS date_last_modified,
    {{ dbt_date.convert_timezone('lastactivitydate', source_tz='America/Los_Angeles') }} AS date_last_activity,
    {{ dbt_date.convert_timezone('lasttransferdate', source_tz='America/Los_Angeles') }} AS date_last_transfer,
    {{ dbt_date.convert_timezone('emailbounceddate', source_tz='America/Los_Angeles') }} AS date_email_bounced,
    {{ dbt_date.convert_timezone('systemmodstamp', source_tz='America/Los_Angeles') }}   AS date_system_mod_stamp

  FROM stage

)

SELECT * FROM reformatted
