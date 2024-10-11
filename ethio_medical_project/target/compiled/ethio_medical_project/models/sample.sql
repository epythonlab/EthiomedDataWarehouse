-- models/ethio_medical.sql
  -- Correcting the materialization syntax

WITH source_data AS (
    SELECT * 
    FROM "medical_data"."public"."ethio_medical"  -- Use DBT's source function
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