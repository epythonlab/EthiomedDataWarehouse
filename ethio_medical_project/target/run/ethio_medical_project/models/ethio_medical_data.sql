
  create view "medical_data"."public"."ethio_medical_data__dbt_tmp"
    
    
  as (
    -- models/ethio_medical_data.sql
WITH source_data AS (
    SELECT * FROM "medical_data"."public"."ethio_medical"  -- Use DBT's ref function to reference another model
)
SELECT
    channel_title,
    lower(channel_username) AS channel_username,
    message_id,
    "Message",
    date,
    media_path
FROM source_data
WHERE "Message" IS NOT NULL
  );