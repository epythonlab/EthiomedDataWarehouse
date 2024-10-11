select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select price_in_birr
from "medical_data"."public"."transformed_medical_product"
where price_in_birr is null



      
    ) dbt_internal_test