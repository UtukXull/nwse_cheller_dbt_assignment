{{ config(alias = 'dim_salesforce_user') }}

WITH base_user AS (

  SELECT * FROM {{ ref('int_salesforce__user') }}

),

user AS (

  SELECT
    bu.user_id,
    bu.username,
    bu.first_name,
    bu.last_name,
    bu.is_user_active,
    bu.company_name,
    bu.country,
    bu.email,
    bu.alias,
    bu.community_nickname,
    bu.is_user_system_controlled,
    bu.locale_sid_key,
    bu.user_type,
    bu.user_subtype,
    bu.language_locale_key,
    {{ transform_date_to_id('date_user_last_login', 'bu') }} AS user_last_login__date_id,
    {{ transform_date_to_id('date_user_last_password_change', 'bu') }} AS user_last_password_change__date_id,
    {{ transform_date_to_id('date_user_created', 'bu') }} AS user_created__date_id,
    bu.created_by__user_id,
    TRIM(CONCAT(cr.first_name,' ',cr.last_name)) AS created_by_name,
    {{ transform_date_to_id('date_user_last_modified', 'bu') }} AS user_last_modified__date_id,
    bu.last_modified_by__user_id,
    TRIM(CONCAT(mod.first_name,' ',mod.last_name)) AS last_modified_by_name,
    bu.number_of_failed_logins,
    bu.user__contact_id,
    bu.user__account_id

  FROM base_user bu
  LEFT JOIN base_user cr
    ON bu.created_by__user_id = cr.user_id
  LEFT JOIN base_user mod
    ON bu.last_modified_by__user_id = mod.user_id

)

SELECT * FROM user
