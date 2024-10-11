select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select contact_phone_numbers
from "medical_data"."public"."transformed_contact_number"
where contact_phone_numbers is null



      
    ) dbt_internal_test