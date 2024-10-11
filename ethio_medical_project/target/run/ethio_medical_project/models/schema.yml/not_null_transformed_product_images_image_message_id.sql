select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select image_message_id
from "medical_data"."public"."transformed_product_images"
where image_message_id is null



      
    ) dbt_internal_test