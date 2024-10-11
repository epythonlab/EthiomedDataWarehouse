
    
    

with child as (
    select channel_username as from_field
    from "medical_data"."public"."transformed_medical_product"
    where channel_username is not null
),

parent as (
    select channel_username as to_field
    from "medical_data"."public"."transformed_channel"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


