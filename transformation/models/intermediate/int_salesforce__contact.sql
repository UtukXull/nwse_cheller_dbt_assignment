with stage AS (

  SELECT * FROM {{ ref('stg_salesforce__contact') }}

),

reformatted AS (

  SELECT
    contact_id,
    salutation,
    phone,
    fax,
    email,
    title,
    department,
    jigsaw,
    pronouns,

-- renaming
    accountid              AS contact__account_id,
    firstname              AS first_name,
    lastname               AS last_name,
    otherstreet            AS other_street,
    othercity              AS other_city,
    otherstate             AS other_state,
    otherpostalcode        AS other_postal_code,
    othercountry           AS other_country,
    otherlatitude          AS other_latitude,
    otherlongitude         AS other_longitude,
    othergeocodeaccuracy   AS other_geocode_accuracy,
    mailingstreet          AS mailing_street,
    mailingcity            AS mailing_city,
    mailingstate           AS mailing_state,
    mailingpostalcode      AS mailing_postal_code,
    mailingcountry         AS mailing_country,
    mailinglatitude        AS mailing_latitude,
    mailinglongitude       AS mailing_longitude,
    mailinggeocodeaccuracy AS mailing_geocode_accuracy,
    mobilephone            AS mobile_phone,
    homephone              AS home_phone,
    otherphone             AS other_phone,
    assistantphone         AS assistant_phone,
    reportstoid            AS contact_reports_to__user_id,
    assistantname          AS assistant_name,
    leadsource             AS lead_source,
    description            AS contact_description,
    ownerid                AS contact_owner__user_id,
    createdbyid            AS contact_created_by__user_id,
    lastmodifiedbyid       AS contact_last_modified_by__user_id,
    emailbouncedreason     AS email_bounced_reason,
    cleanstatus            AS clean_status,
    genderidentity         AS gender_identity,
    level__c               AS level,
    languages__c           AS languages,

-- boolean conversions
    {{ convert_to_boolean('isdeleted') }}          AS is_contact_deleted,
    {{ convert_to_boolean('hasoptedoutofemail') }} AS has_contact_opted_out_of_email,
    {{ convert_to_boolean('hasoptedoutoffax') }}   AS has_contact_opted_out_of_fax,
    {{ convert_to_boolean('donotcall') }}          AS has_contact_do_not_call,

-- date conversions
    {{ dbt_date.convert_timezone('birthdate', source_tz='America/Los_Angeles') }}         AS date_of_contact_birth,
    {{ dbt_date.convert_timezone('createddate', source_tz='America/Los_Angeles') }}       AS date_contact_created,
    {{ dbt_date.convert_timezone('lastmodifieddate', source_tz='America/Los_Angeles') }}  AS date_contact_last_modified,
    {{ dbt_date.convert_timezone('lastactivitydate', source_tz='America/Los_Angeles') }}  AS date_contact_last_activity,
    {{ dbt_date.convert_timezone('lastcurequestdate', source_tz='America/Los_Angeles') }} AS date_contact_last_cu_request,
    {{ dbt_date.convert_timezone('lastcuupdatedate', source_tz='America/Los_Angeles') }}  AS date_contact_last_cu_update,
    {{ dbt_date.convert_timezone('emailbounceddate', source_tz='America/Los_Angeles') }}  AS date_contact_email_bounced,
    {{ dbt_date.convert_timezone('systemmodstamp', source_tz='America/Los_Angeles') }}    AS date_contact_system_mod_stamp

  FROM stage

)

SELECT * FROM reformatted