{{ config(alias = 'dim_dates') }}

WITH base_dates AS (

  {{ dbt_date.get_base_dates(start_date="2018-01-01", end_date="2030-12-31") }}

),

transform AS (

  SELECT
    (EXTRACT(YEAR FROM date_day) * 10000) + (EXTRACT(MONTH FROM date_day) * 100) + EXTRACT(DAY FROM date_day) AS date_id,
    date_day :: DATE AS date_day,
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(MONTH FROM date_day) AS month,
    {{ dbt_date.month_name("date_day", short=false) }} AS month_name,
    EXTRACT(DAY FROM date_day) AS day,
    {{ dbt_date.day_name("date_day", short=false) }} AS day_name,
    EXTRACT(WEEK FROM date_day) AS week,
    CASE
        WHEN EXTRACT(DOW FROM date_day) = 0 THEN 7
        ELSE EXTRACT(DOW FROM date_day)
    END AS day_of_week,
    EXTRACT(QUARTER FROM date_day) AS quarter,
    CASE
        WHEN EXTRACT(DAYOFWEEK FROM date_day) IN (0, 6) THEN TRUE
        ELSE FALSE
    END AS is_weekend,
    CASE
        WHEN EXTRACT(MONTH FROM date_day) >= 4 THEN EXTRACT(YEAR FROM date_day)
        ELSE EXTRACT(YEAR FROM date_day) - 1
    END AS fiscal_year,
    CASE
        WHEN EXTRACT(MONTH FROM date_day) IN (4, 5, 6) THEN 1
        WHEN EXTRACT(MONTH FROM date_day) IN (7, 8, 9) THEN 2
        WHEN EXTRACT(MONTH FROM date_day) IN (10, 11, 12) THEN 3
        ELSE 4
    END AS fiscal_quarter

  FROM base_dates

)

SELECT
  date_id,
  date_day,
  year,
  month,
  month_name,
  day,
  day_name,
  week,
  day_of_week,
  quarter,
  is_weekend,
  fiscal_year,
  fiscal_quarter

FROM transform
