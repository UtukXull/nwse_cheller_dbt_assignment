

with stage AS (

  SELECT * FROM {{ ref('stg_salesforce__solution') }}

),

reformatted AS (

  SELECT

--keys
    solution_id,
    caseid           AS case_id,
    ownerid          AS solution_owner__user_id,
    createdbyid      AS created_by__user_id,
    lastmodifiedbyid AS last_modified_by__user_id,

-- renamed fields
    solutionnumber   AS solution_number,
    solutionname     AS solution_name,
    status           AS solution_status,
    solutionnote     AS solution_note,
    timesused        AS times_used,

-- boolean conversions
    {{ convert_to_boolean('isdeleted') }}             AS is_deleted,
    {{ convert_to_boolean('ispublished') }}           AS is_published,
    {{ convert_to_boolean('ispublishedinpublickb') }} AS is_published_in_public_kb,
    {{ convert_to_boolean('isreviewed') }}            AS is_reviewed,
    {{ convert_to_boolean('ishtml') }}                AS is_html,

-- date conversions
    {{ dbt_date.convert_timezone('createddate', source_tz='America/Los_Angeles') }}      AS date_created,
    {{ dbt_date.convert_timezone('lastmodifieddate', source_tz='America/Los_Angeles') }} AS date_last_modified,
    {{ dbt_date.convert_timezone('systemmodstamp', source_tz='America/Los_Angeles') }}   AS date_system_mod_stamp

  FROM stage

)

SELECT * FROM reformatted
