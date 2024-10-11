
    
    

select
    channel_username as unique_field,
    count(*) as n_records

from "medical_data"."public"."transformed_contact_number"
where channel_username is not null
group by channel_username
having count(*) > 1


