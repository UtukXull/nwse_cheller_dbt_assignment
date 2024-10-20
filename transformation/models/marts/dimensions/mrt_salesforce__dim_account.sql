{{ config(alias = 'dim_salesforce_account') }}

WITH base_account AS (

  SELECT * FROM {{ ref('int_salesforce__account') }}

),

base_user AS (

  SELECT * FROM {{ ref('int_salesforce__user') }}

),

account AS (

  SELECT
    ba.account_id,
    {{ transform_date_to_id('date_account_created', 'ba') }} AS account_created__date_id,
    {{ transform_date_to_id('date_account_last_modified', 'ba') }} AS account_last_modified__date_id,
    {{ transform_date_to_id('date_account_last_activity', 'ba') }} AS account_last_activity__date_id,
    {{ transform_date_to_id('date_account_sla_expiration', 'ba') }} AS account_sla_expiration__date_id,
    ba.account_owner__user_id,
    ba.account_created_by__user_id,
    ba.account_last_modified_by__user_id,
    ba.account_name,
    ba.account_type,
    ba.upsell_opportunity,
    ba.billing_street,
    ba.billing_city,
    ba.billing_state,
    ba.billing_postal_code,
    ba.billing_country,
    ba.shipping_street,
    ba.shipping_city,
    ba.shipping_state,
    ba.shipping_postal_code,
    ba.shipping_country,
    ba.phone,
    ba.fax,
    ba.account_number,
    ba.website,
    ba.sic,
    ba.industry,
    ba.annual_revenue,
    ba.number_of_employees,
    ba.account_ownership,
    ba.ticker_symbol,
    ba.account_description,
    ba.account_rating,
    TRIM(CONCAT(ow.first_name,' ',ow.last_name)) AS account_owner_name,
    TRIM(CONCAT(cr.first_name,' ',cr.last_name)) AS account_created_by_name,
    TRIM(CONCAT(mo.first_name,' ',mo.last_name)) AS account_last_modified_by_name,
    ba.sla,
    ba.sla_serial_number,
    ba.clean_status,
    ba.customer_priority,
    ba.number_of_locations

  FROM base_account ba
  LEFT JOIN base_user ow
    ON ba.account_owner__user_id = ow.user_id
  LEFT JOIN base_user cr
    ON ba.account_created_by__user_id = cr.user_id
  LEFT JOIN base_user mo
    ON ba.account_last_modified_by__user_id = mo.user_id

)

SELECT * FROM account
