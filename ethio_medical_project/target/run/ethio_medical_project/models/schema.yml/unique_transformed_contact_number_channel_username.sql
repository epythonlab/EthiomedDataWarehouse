select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    channel_username as unique_field,
    count(*) as n_records

from "medical_data"."public"."transformed_contact_number"
where channel_username is not null
group by channel_username
having count(*) > 1



      
    ) dbt_internal_test