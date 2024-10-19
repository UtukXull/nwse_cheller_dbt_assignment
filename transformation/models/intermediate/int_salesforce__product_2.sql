

with stage AS (

  SELECT * FROM {{ ref('stg_salesforce__product_2') }}

),

reformatted AS (

  SELECT
    product_id,

-- renaming
    name                  AS product_name,
    productcode           AS product_code,
    description           AS product_description,
    createdbyid           AS created_by__user_id,
    lastmodifiedbyid      AS last_modified_by__user_id,
    family                AS product_family,
    externaldatasourceid  AS external_data_source_id,
    externalid            AS external_id,
    displayurl            AS display_url,
    quantityunitofmeasure AS quantity_unit_of_measure,
    stockkeepingunit      AS stock_keeping_unit,
    type                  AS product_type,
    productclass          AS product_class,
    sourceproductid       AS source__product_id,
    sellerid              AS seller_id,

-- boolean conversions
    {{ convert_to_boolean('isactive') }}   AS is_active,
    {{ convert_to_boolean('isdeleted') }}  AS is_deleted,
    {{ convert_to_boolean('isarchived') }} AS is_archived,

-- date conversions
    {{ dbt_date.convert_timezone('createddate', source_tz='America/Los_Angeles') }}      AS date_created,
    {{ dbt_date.convert_timezone('lastmodifieddate', source_tz='America/Los_Angeles') }} AS date_last_modified,
    {{ dbt_date.convert_timezone('systemmodstamp', source_tz='America/Los_Angeles') }}   AS date_system_mod_stamp

  FROM stage

)

SELECT * FROM reformatted
