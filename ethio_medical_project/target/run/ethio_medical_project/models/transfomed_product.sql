
  
    

  create  table "medical_data"."public"."transfomed_product__dbt_tmp"
  
  
    as
  
  (
    -- models/transformed_product_images.sql


WITH source_data AS (
    SELECT
        channel_id,
        message_id as product_id,
        product_name,
        price_in_birr  -- Rename the column to be clearer
    FROM "medical_data"."public"."transformed_medical_product"
)

SELECT   -- Ensure unique channel usernames
    channel_id,
    product_id,
    product_name,
    price_in_birr  -- Rename the column to be clearer
FROM source_data
  );
  