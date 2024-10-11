-- models/transformed_product_images.sql


WITH source_data AS (
    SELECT 
        message_id,
        media_path  -- Assuming this column contains the image paths
    FROM "medical_data"."public"."transformed_medical_product"
    WHERE media_path IS NOT NULL AND media_path != ''  -- Filter out any null or empty paths
)

SELECT
    message_id,
    media_path AS image_path  -- Rename the column to be clearer
FROM source_data