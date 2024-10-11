
  
    

  create  table "medical_data"."public"."transformed_medical__dbt_tmp"
  
  
    as
  
  (
    -- models/transform_phone_product_price.sql


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
        -- Clean the message of phone numbers and YouTube links
        regexp_replace(
            regexp_replace("Message", '09\s*[0-9]{8}', '', 'g'),
            '(https?://)?(www\.)?(youtube\.com/watch\?v=|youtu\.be/)([a-zA-Z0-9_-]+)(\?si=[a-zA-Z0-9_-]+)?', 
            '', 
            'g'
        ) AS cleaned_message
    FROM source_data
),
product_price_extracted AS (
    SELECT
        message_id,
        channel_username,
        date,
        TRIM(cleaned_message) AS cleaned_message,
        phone_numbers,
        -- Adjusted regex to capture product names and prices
        regexp_matches(cleaned_message, '^(.*?)\s*(?:price|Price|PRICE)\s*(\d+)\s*(birr|ETB)', 'g') AS matches,
        ROW_NUMBER() OVER (PARTITION BY message_id ORDER BY date DESC) AS rn  -- Add row number to filter
    FROM phone_and_link_extracted
)

SELECT
    message_id,
    channel_username,
    date,
    TRIM(matches[1]) AS product,  -- Extract product name
    CAST(TRIM(matches[2]) AS INTEGER) AS price_in_birr,  -- Extract price as an integer
    CASE
        WHEN phone_numbers IS NOT NULL AND phone_numbers != '' THEN phone_numbers
        ELSE NULL
    END AS phone_numbers
FROM product_price_extracted
WHERE matches IS NOT NULL  -- Filter out any rows where matches are not found
AND rn = 1  -- Select only the secound occurrence of each message_id
ORDER BY message_id  -- Optional: order by message_id
  );
  