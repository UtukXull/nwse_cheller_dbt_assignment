WITH stage AS (

  SELECT * FROM {{ ref('stg_salesforce__user') }}

),

reformatted AS (

  SELECT

--keys
    user_id,
    contactid           AS user__contact_id,
    accountid           AS user__account_id,
    delegatedapproverid AS delegated_approver__user_id,
    managerid           AS manager__user_id,
    createdbyid         AS created_by__user_id,
    lastmodifiedbyid    AS last_modified_by__user_id,

--direct fields
    username,
    division,
    department,
    title,
    street,
    city,
    state,
    country,
    latitude,
    longitude,
    email,
    signature,

-- renamed fields
    firstname                         AS first_name,
    lastname                          AS last_name,
    companyname                       AS company_name,
    postalcode                        AS postal_code,
    geocodeaccuracy                   AS geocode_accuracy,
    senderemail                       AS sender_email,
    sendername                        AS sender_name,
    stayintouchsubject                AS stay_in_touch_subject,
    stayintouchsignature              AS stay_in_touch_signature,
    stayintouchnote                   AS stay_in_touch_note,
    phone                             AS phone,
    fax                               AS fax,
    mobilephone                       AS mobile_phone,
    alias                             AS alias,
    communitynickname                 AS community_nickname,
    timezonesidkey                    AS time_zone_sid_key,
    localesidkey                      AS locale_sid_key,
    emailencodingkey                  AS email_encoding_key,
    usertype                          AS user_type,
    usersubtype                       AS user_subtype,
    startday                          AS start_day,
    endday                            AS end_day,
    languagelocalekey                 AS language_locale_key,
    employeenumber                    AS employee_number,
    numberoffailedlogins              AS number_of_failed_logins,
    extension                         AS extension,
    federationidentifier              AS federation_identifier,
    aboutme                           AS about_me,
    loginlimit                        AS login_limit,
    digestfrequency                   AS digest_frequency,
    defaultgroupnotificationfrequency AS default_group_notification_frequency,
    jigsawimportlimitoverride         AS jigsaw_import_limit_override,
    sharingtype                       AS sharing_type,
    chatteradoptionstage              AS chatter_adoption_stage,
    globalidentity                    AS global_identity,

-- boolean conversions
    {{ convert_to_boolean('isactive') }}                AS is_user_active,
    {{ convert_to_boolean('forecastenabled') }}         AS is_user_forecast_enabled,
    {{ convert_to_boolean('isprofilephotoactive') }}    AS is_user_profile_photo_active,
    {{ convert_to_boolean('issystemcontrolled') }}      AS is_user_system_controlled,
    {{ convert_to_boolean('receivesadmininfoemails') }} AS has_user_receive_admin_info_emails,
    {{ convert_to_boolean('receivesinfoemails') }}      AS has_user_receive_info_emails,

-- date conversions
    {{ dbt_date.convert_timezone('chatteradoptionstagemodifieddate', source_tz='America/Los_Angeles') }} AS date_user_chatter_adoption_stage_modified,
    {{ dbt_date.convert_timezone('createddate', source_tz='America/Los_Angeles') }}                      AS date_user_created,
    {{ dbt_date.convert_timezone('lastlogindate', source_tz='America/Los_Angeles') }}                    AS date_user_last_login,
    {{ dbt_date.convert_timezone('lastmodifieddate', source_tz='America/Los_Angeles') }}                 AS date_user_last_modified,
    {{ dbt_date.convert_timezone('lastpasswordchangedate', source_tz='America/Los_Angeles') }}           AS date_user_last_password_change,
    {{ dbt_date.convert_timezone('offlinepdatrialexpirationdate', source_tz='America/Los_Angeles') }}    AS date_user_offline_pda_trial_expiration,
    {{ dbt_date.convert_timezone('offlinetrialexpirationdate', source_tz='America/Los_Angeles') }}       AS date_user_offline_trial_expiration,
    {{ dbt_date.convert_timezone('suaccessexpirationdate', source_tz='America/Los_Angeles') }}           AS date_user_su_access_expiration,
    {{ dbt_date.convert_timezone('suorgadminexpirationdate', source_tz='America/Los_Angeles') }}         AS date_user_su_org_admin_expiration,
    {{ dbt_date.convert_timezone('wirelesstrialexpirationdate', source_tz='America/Los_Angeles') }}      AS date_user_wireless_trial_expiration,
    {{ dbt_date.convert_timezone('systemmodstamp', source_tz='America/Los_Angeles') }}                   AS date_user_system_mod_stamp

  FROM stage

)

SELECT * FROM reformatted
