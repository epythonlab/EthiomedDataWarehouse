
    
    

select
    channel_id as unique_field,
    count(*) as n_records

from "medical_data"."public"."transformed_contact_number"
where channel_id is not null
group by channel_id
having count(*) > 1


