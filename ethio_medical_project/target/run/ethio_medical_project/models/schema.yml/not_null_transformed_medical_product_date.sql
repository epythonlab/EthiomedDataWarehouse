select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select date
from "medical_data"."public"."transformed_medical_product"
where date is null



      
    ) dbt_internal_test