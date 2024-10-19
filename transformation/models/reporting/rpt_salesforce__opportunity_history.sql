-- Creating a One Big Table (OBT) that aggregates relevant metrics for Salesforce Opportunity History

{{ config(alias = 'report_salesforce_opportunity_history') }}

WITH base_opportunity_history AS (

  SELECT * FROM {{ ref('mrt_salesforce__fact_opportunity_history') }}

),

base_date_dim AS (

  SELECT * FROM {{ ref('mrt__dim_date') }}

),

base_account_dim AS (

  SELECT * FROM {{ ref('mrt_salesforce__dim_account') }}

),

base_user_dim AS (

  SELECT * FROM {{ ref('mrt_salesforce__dim_user') }}

),

base_opportunity_dim AS (

  SELECT * FROM {{ ref('mrt_salesforce__dim_opportunity') }}

),

base_opportunity_history_dim AS (

  SELECT * FROM {{ ref('mrt_salesforce__dim_opportunity_history') }}

),

opportunity_history AS (
    SELECT
        opportunity_id,
        opportunity_type,
        opportunity_amount,
        date_opportunity_record_valid_from,
        account_id,
        opportunity_owner__user_id,
        opportunity_created_by__user_id
    FROM base_opportunity_history
    WHERE
        NOT is_account_deleted
        AND is_account_active
),

date_dim AS (
    SELECT
        date_id,
        date_day,
        day_of_week,
        is_weekend
    FROM base_date_dim
),

account_dim AS (
    SELECT
        account_id,
        sla,
        account_sla_expiration__date_id,
        industry
    FROM base_account_dim
),

user_dim AS (
    SELECT
        user_id,
        CONCAT(first_name, ' ', last_name) AS full_name,
        is_user_active
    FROM base_user_dim
),

opportunity_dim AS (
    SELECT
        opportunity_id,
        opportunity_name,
        opportunity_stage_name AS stage_name,
        opportunity_order_number AS order_number
    FROM base_opportunity_dim
),

opportunity_history_dim AS (
    SELECT
        opportunity_id,
        opportunity_forecast_category,
        date_opportunity_record_valid_from
    FROM base_opportunity_history_dim
),

-- Join opportunity history with account, user, opportunity, and date dimensions
joined_data AS (
    SELECT
        oh.opportunity_id,
        od.opportunity_name,
        oh.opportunity_type,
        oh.account_id,
        oh.opportunity_owner__user_id,
        ud1.full_name AS opportunity_owner_name,
        ud1.is_user_active AS is_opportunity_owner_active,
        oh.opportunity_created_by__user_id,
        ud2.full_name AS opportunity_created_by_user_name,
        ud2.is_user_active AS is_opportunity_created_by_user_active,
        ad.sla AS account_sla,
        dd.date_day AS date_account_sla_expiration,
        dd.day_of_week AS account_sla_expiration_day_of_week,
        dd.is_weekend AS is_account_sla_expiration_weekend,
        ad.industry AS account_industry,
        od.stage_name,
        od.order_number,
        oph.opportunity_forecast_category,
        oh.opportunity_amount,

    FROM opportunity_history oh
    LEFT JOIN account_dim ad
      ON oh.account_id = ad.account_id
    LEFT JOIN user_dim ud1
      ON oh.opportunity_owner__user_id = ud1.user_id
    LEFT JOIN user_dim ud2
      ON oh.opportunity_created_by__user_id = ud2.user_id
    LEFT JOIN opportunity_dim od
      ON oh.opportunity_id = od.opportunity_id
    LEFT JOIN opportunity_history_dim oph
      ON oh.opportunity_id = oph.opportunity_id
      AND oh.date_opportunity_record_valid_from = oph.date_opportunity_record_valid_from
    LEFT JOIN date_dim dd
      ON ad.account_sla_expiration__date_id = dd.date_id
),

-- Aggregate metrics for Opportunities
aggregated_metrics AS (
    SELECT
        opportunity_type,
        COUNT(DISTINCT opportunity_id) AS total_opportunities,
        SUM(opportunity_amount) AS total_opportunity_amount
    FROM joined_data
    GROUP BY opportunity_type
),

final_obt AS (
    SELECT
        jd.opportunity_id,
        jd.opportunity_name,
        jd.opportunity_type,
        jd.account_id,
        jd.opportunity_owner__user_id,
        jd.opportunity_owner_name,
        jd.is_opportunity_owner_active,
        jd.opportunity_created_by__user_id,
        jd.opportunity_created_by_user_name,
        jd.is_opportunity_created_by_user_active,
        jd.account_sla,
        jd.date_account_sla_expiration,
        jd.account_sla_expiration_day_of_week,
        jd.is_account_sla_expiration_weekend,
        jd.account_industry,
        jd.stage_name,
        jd.order_number,
        jd.opportunity_forecast_category,
        am.total_opportunities,
        am.total_opportunity_amount
    FROM aggregated_metrics am
    LEFT JOIN joined_data jd ON am.opportunity_type = jd.opportunity_type
)

SELECT * FROM final_obt
