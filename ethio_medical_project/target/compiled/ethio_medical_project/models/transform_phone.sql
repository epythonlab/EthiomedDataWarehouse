-- models/transform_phone.sql
  -- Use table materialization for the model

WITH source_data AS (
    SELECT * 
    FROM "medical_data"."public"."ethio_medical"
)

SELECT
    message_id,
    lower(channel_username) AS channel_username,
    "Message",
    date,
    media_path,
    -- Extracting all phone numbers that match the pattern
    CASE
        WHEN regexp_matches("Message", '\b(09[0-9]{8})\b', 'g') IS NOT NULL 
        THEN array_to_string(regexp_matches("Message", '\b(09[0-9]{8})\b', 'g'), ', ')
        ELSE NULL
    END AS phone_numbers
FROM source_data
WHERE "Message" IS NOT NULL