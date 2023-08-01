{{config(
    post_hook="update {{this}} set year_of_birth = (select median(year_of_birth) from {{this}}) 
    where year_of_birth is null"
    )}}
select
    msisdn::varchar(256) as msisdn,
    case 
        when startswith(lower(gender),'m') or startswith(lower(gender),'f')
            then
                case
                    when editdistance(lower(gender),'male')<editdistance(lower(gender),'female') then 'Male'
                    else 'Female'
                end
        else 'Unknown'
    end as gender,
    year_of_birth::int as year_of_birth,
    mobile_type::varchar(256) as mobile_type,
    value_segment::varchar(256) as value_segment,
    system_status::varchar(256) as system_status
from {{source('staging_layer','crm')}}
where year_of_birth::int <= year(current_timestamp)