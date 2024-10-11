
  
    

  create  table "medical_data"."public"."transformed_phone__dbt_tmp"
  
  
    as
  
  (
    -- models/transform_phone.sql
  -- Use table materialization for the model

WITH source_data AS (
    SELECT * 
    FROM "medical_data"."public"."ethio_medical"
),
phone_and_link_extracted AS (
    SELECT
        message_id,
        lower(channel_username) AS channel_username,
        "Message",
        date,
        media_path,
        -- Extract phone numbers with optional spaces
        array_to_string(ARRAY(
            SELECT regexp_replace(unnest(regexp_matches("Message", '09\s*[0-9]{8}', 'g')), '\s+', '', 'g')
        ), ', ') AS phone_numbers,
        -- Remove phone numbers and YouTube links from the original message
        regexp_replace(
            regexp_replace("Message", '09\s*[0-9]{8}', '', 'g'),
            '(https?://)?(www\.)?(youtube\.com/watch\?v=|youtu\.be/)([a-zA-Z0-9_-]+)(\?si=[a-zA-Z0-9_-]+)?', 
            '', 
            'g'
        ) AS cleaned_message,
        -- Extract YouTube links and clean spaces
        array_to_string(ARRAY(
            SELECT regexp_replace(unnest(regexp_matches("Message", '(https?://)?(www\.)?(youtube\.com/watch\?v=|youtu\.be/)([a-zA-Z0-9_-]+)', 'g')), '\s+', '', 'g')
        ), '') AS youtube_links
    FROM source_data
)

SELECT
    message_id,
    channel_username,
    cleaned_message AS "Message",  -- Use the cleaned message without phone numbers and YouTube links
    date,
    media_path,
    CASE
        WHEN phone_numbers IS NOT NULL AND phone_numbers != '' THEN phone_numbers
        ELSE NULL
    END AS phone_numbers,
    CASE
        WHEN youtube_links IS NOT NULL AND youtube_links != '' THEN youtube_links
        ELSE NULL
    END AS youtube_links
FROM phone_and_link_extracted
  );
  