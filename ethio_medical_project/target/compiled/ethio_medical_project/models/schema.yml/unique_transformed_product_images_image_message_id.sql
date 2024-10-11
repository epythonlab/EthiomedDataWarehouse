
    
    

select
    image_message_id as unique_field,
    count(*) as n_records

from "medical_data"."public"."transformed_product_images"
where image_message_id is not null
group by image_message_id
having count(*) > 1


