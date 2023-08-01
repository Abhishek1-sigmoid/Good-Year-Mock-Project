select
    msisdn::varchar(256) as msisdn,
    imei_tac::varchar(256) as imei_tac,
    coalesce(brand_name::varchar(256),'APPLE') as brand_name,
    model_name::varchar(256) as model_name,
    case
        when os_name is null
            then
                case 
                    when model_name is null then 'iOS'
                    else 'Android'
                end
        else os_name::varchar(256)
    end as os_name,
    case 
        when os_vendor is null
            then
                case 
                    when model_name is null then 'Apple'
                    else 'Google'
                end
        else os_vendor::varchar(256)
    end as os_vendor
from {{source('staging_layer','devices')}}
