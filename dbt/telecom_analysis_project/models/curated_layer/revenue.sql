select
    MSISDN::varchar(256) as MSISDN,
    WEEK_NUBMER::int as WEEK_NUBMER,
    REVENUE::decimal(20,2) as REVENUE
from {{source('staging_layer','revenue')}}
