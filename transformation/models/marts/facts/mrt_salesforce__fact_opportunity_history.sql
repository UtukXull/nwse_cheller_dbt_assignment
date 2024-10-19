{{ config(alias='fact_salesforce_opportunity_history') }}

WITH base_opportunity_history AS (

  SELECT * FROM {{ ref('int_salesforce__opportunity_history') }}

),

base_opportunity AS (

  SELECT * FROM {{ ref('int_salesforce__opportunity') }}

),

base_account AS (

  SELECT * FROM {{ ref('int_salesforce__account') }}

),

base_contact AS (

  SELECT * FROM {{ ref('int_salesforce__contact') }}

),

opportunity_history AS (

  SELECT
    opportunity_id,
    opportunity_created_by__user_id,
    {{ transform_date_to_id('date_opportunity_created') }} AS opportunity_created_date__id,
    date_opportunity_created :: TIME AS time_opportunity_created,
    opportunity_amount,
    opportunity_expected_revenue,
    LAG(opportunity_probability) OVER (PARTITION BY opportunity_id ORDER BY date_opportunity_record_valid_through ASC) AS opportunity_previous_probability,
    {{ transform_date_to_id('date_opportunity_close') }} AS opportunity_close_date__id,
    date_opportunity_close :: TIME AS time_opportunity_close,
    is_opportunity_deleted,
    LAG(opportunity_amount) OVER (PARTITION BY opportunity_id ORDER BY date_opportunity_record_valid_through ASC) AS opportunity_previous_amount,
    date_opportunity_record_valid_from,
    date_opportunity_record_valid_through

  FROM base_opportunity_history

),

opportunity AS (

  SELECT
    opportunity_id,
    opportunity_type,
    account_id,
    opportunity_owner__user_id

  FROM base_opportunity

),

account AS (

  SELECT
    account_id,
    is_account_deleted,
    account_type,
    is_account_active

  FROM base_account

),

final AS (

  SELECT
    oph.opportunity_id,
    oph.date_opportunity_record_valid_from,
    oph.date_opportunity_record_valid_through,
    op.opportunity_type,
    op.account_id,
    ophac.account_type,
    ophac.is_account_deleted,
    ophac.is_account_active,
    op.opportunity_owner__user_id,
    oph.opportunity_created_by__user_id,
    oph.opportunity_created_date__id,
    oph.time_opportunity_created,
    oph.opportunity_amount,
    oph.opportunity_previous_amount,
    oph.opportunity_expected_revenue,
    ((oph.opportunity_previous_probability/100) * oph.opportunity_previous_amount) AS opportunity_previous_expected_revenue,
    oph.opportunity_close_date__id,
    oph.time_opportunity_close,
    oph.is_opportunity_deleted

  FROM opportunity_history oph
  LEFT JOIN opportunity op
    ON oph.opportunity_id = op.opportunity_id
  LEFT JOIN account ophac
    ON op.account_id = ophac.account_id

)

SELECT * FROM final
