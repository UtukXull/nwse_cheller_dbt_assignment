

with stage AS (

  SELECT * FROM {{ ref('stg_salesforce__pricebook_entry') }}

),

reformatted AS (

  SELECT

--keys
    pricebook_entry_id,
    pricebook2id     AS pricebook_id,
    product2id       AS product_id,
    createdbyid      AS created_by__user_id,
    lastmodifiedbyid AS last_modified_by__user_id,

-- renamed fields
    unitprice AS unit_price,

-- boolean conversions
    {{ convert_to_boolean('isdeleted') }}        AS is_deleted,
    {{ convert_to_boolean('isarchived') }}       AS is_archived,
    {{ convert_to_boolean('isactive') }}         AS is_active,
    {{ convert_to_boolean('usestandardprice') }} AS uses_standard_price,

-- date conversions
    {{ dbt_date.convert_timezone('createddate', source_tz='America/Los_Angeles') }}      AS date_created,
    {{ dbt_date.convert_timezone('lastmodifieddate', source_tz='America/Los_Angeles') }} AS date_last_modified,
    {{ dbt_date.convert_timezone('systemmodstamp', source_tz='America/Los_Angeles') }}   AS date_system_mod_stamp

  FROM stage

)

SELECT * FROM reformatted
