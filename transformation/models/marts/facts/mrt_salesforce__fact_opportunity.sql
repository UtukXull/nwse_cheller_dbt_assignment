{{ config(alias = 'fact_salesforce_opportunity') }}

WITH base_opportunity AS (

  SELECT * FROM {{ ref('int_salesforce__opportunity') }}

),

base_account AS (

  SELECT * FROM {{ ref('int_salesforce__account') }}

),

base_contact AS (

  SELECT * FROM {{ ref('int_salesforce__contact') }}

),

opportunity AS (

  SELECT
    opportunity_id,
    opportunity_type,
    account_id,
    opportunity_amount,
    opportunity_expected_revenue,
    opportunity_created_by__user_id,
    {{ transform_date_to_id('date_opportunity_created') }} AS opportunity_created__date_id,
    date_opportunity_created :: TIME AS time_opportunity_created,
    opportunity_last_modified_by__user_id,
    {{ transform_date_to_id('date_opportunity_last_modified') }} AS opportunity_last_modified__date_id,
    date_opportunity_last_modified :: TIME AS time_opportunity_last_modified,
    {{ transform_date_to_id('date_opportunity_close') }} AS opportunity_close__date_id,
    date_opportunity_close :: TIME AS time_opportunity_close,
    is_opportunity_closed,
    is_opportunity_won,
    opportunity_owner__user_id,
    is_opportunity_deleted

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
    op.opportunity_id,
    op.account_id,
    op.opportunity_created_by__user_id,
    op.opportunity_owner__user_id,
    op.opportunity_created__date_id,
    op.opportunity_last_modified__date_id,
    op.opportunity_close__date_id,
    op.opportunity_type,
    op.opportunity_amount,
    op.opportunity_expected_revenue,
    opac.account_type,
    opac.is_account_deleted,
    opac.is_account_active,
    op.time_opportunity_created,
    op.time_opportunity_last_modified,
    op.time_opportunity_close,
    op.is_opportunity_closed,
    op.is_opportunity_won,
    op.is_opportunity_deleted

  FROM opportunity op
  LEFT JOIN account opac
    ON op.account_id = opac.account_id

)

SELECT * FROM final
