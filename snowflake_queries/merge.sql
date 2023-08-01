create schema if not exists OUTPUT;

create or replace table TELECOM_ANALYSIS.OUTPUT.OUTPUT as
with a_weekwise as (
    select
        'total_devices_weekwise' as "desc",
        null as gender,
        null as age,
        revenue.week_nubmer,
        null as value_segment,
        count(*) as devices,
        null as active_devices,
        null as mobile_type,
        null as brand_name,
        null as os_name,
        null as os_vendor,
        null as customer,
        null as min_revenue,
        null as max_revenue,
        null as revenue_generated
    from TELECOM_ANALYSIS.DBT_CURATED.DEVICES as devices
    inner join TELECOM_ANALYSIS.DBT_CURATED.REVENUE as revenue on devices.msisdn = revenue.msisdn
    group by week_nubmer 
    order by week_nubmer
), a_weekwise_active as (
    select
        'total_active_devices_weekwise' as "desc",
        null as gender,
        null as age,
        revenue.week_nubmer,
        null as value_segment,
        null as devices,
        count(*) as active_devices,
        null as mobile_type,
        null as brand_name,
        null as os_name,
        null as os_vendor,
        null as customer,
        null as min_revenue,
        null as max_revenue,
        null as revenue_generated
    from TELECOM_ANALYSIS.DBT_CURATED.CRM as crm
    inner join TELECOM_ANALYSIS.DBT_CURATED.DEVICES as devices on crm.msisdn = devices.msisdn
    inner join TELECOM_ANALYSIS.DBT_CURATED.REVENUE as revenue on devices.msisdn = revenue.msisdn
    where crm.system_status = 'ACTIVE'
    group by week_nubmer
    order by week_nubmer
), b_weekwise as (
    select
        'total_customer_weekwise' as "desc",
        null as gender,
        null as age,
        revenue.week_nubmer,
        null as value_segment,
        null as devices,
        null as active_devices,
        null as mobile_type,
        null as brand_name,
        null as os_name,
        null as os_vendor,
        count(*) as customer,
        null as min_revenue,
        null as max_revenue,
        null as revenue_generated
    from TELECOM_ANALYSIS.DBT_CURATED.DEVICES as devices
    inner join TELECOM_ANALYSIS.DBT_CURATED.REVENUE as revenue on devices.msisdn = revenue.msisdn
    group by week_nubmer 
    order by week_nubmer
), b_weekwise_active as (
    select
        'total_customer_weekwise_active' as "desc",
        null as gender,
        null as age,
        revenue.week_nubmer,
        null as value_segment,
        null as devices,
        null as active_devices,
        null as mobile_type,
        null as brand_name,
        null as os_name,
        null as os_vendor,
        count(*) as customer,
        null as min_revenue,
        null as max_revenue,
        null as revenue_generated
    from TELECOM_ANALYSIS.DBT_CURATED.CRM as crm
    inner join TELECOM_ANALYSIS.DBT_CURATED.DEVICES as devices on crm.msisdn = devices.msisdn
    inner join TELECOM_ANALYSIS.DBT_CURATED.REVENUE as revenue on devices.msisdn = revenue.msisdn
    where crm.system_status = 'ACTIVE'
    group by week_nubmer 
    order by week_nubmer
), c_overall as (
    select
        'revenue_overall_male_female' as "desc",
        crm.gender,
        null as age,
        null as week_nubmer,
        null as value_segment,
        null as devices,
        null as active_devices,
        null as mobile_type,
        null as brand_name,
        null as os_name,
        null as os_vendor,
        null as customer,
        null as min_revenue,
        null as max_revenue,
        sum(revenue.revenue) as revenue_generated
    from TELECOM_ANALYSIS.DBT_CURATED.CRM as crm
    inner join TELECOM_ANALYSIS.DBT_CURATED.REVENUE as revenue on crm.msisdn = revenue.msisdn
    where crm.gender <> 'Unknown'
    group by gender
    order by revenue_generated
), c_weekwise as (
    select
        'revenue_weekwise_male_female' as "desc",
        crm.gender as gender,
        null as age,
        revenue.week_nubmer as week_nubmer,
        null as value_segment,
        null as devices,
        null as active_devices,
        null as mobile_type,
        null as brand_name,
        null as os_name,
        null as os_vendor,
        null as customer,
        null as min_revenue,
        null as max_revenue,
        sum(revenue.revenue) as revenue_generated
    from TELECOM_ANALYSIS.DBT_CURATED.CRM as crm
    inner join TELECOM_ANALYSIS.DBT_CURATED.REVENUE as revenue on crm.msisdn = revenue.msisdn
    where crm.gender <> 'Unknown'
    group by gender,week_nubmer
    order by gender,week_nubmer
), d_overall as (
    select
        'revenue_overall_by_age' as "desc",
        null as gender,
        year(current_timestamp)-crm.year_of_birth as age,
        null as week_nubmer,
        null as value_segment,
        null as devices,
        null as active_devices,
        null as mobile_type,
        null as brand_name,
        null as os_name,
        null as os_vendor,
        null as customer,
        null as min_revenue,
        null as max_revenue,
        sum(revenue.revenue) as revenue_generated
    from TELECOM_ANALYSIS.DBT_CURATED.CRM as crm
    inner join TELECOM_ANALYSIS.DBT_CURATED.REVENUE as revenue on crm.msisdn = revenue.msisdn
    group by age 
    order by revenue_generated desc
), d_weekwise as (
    select
        'revenue_weekwise_by_age' as "desc",
        null as gender,
        year(current_timestamp)-crm.year_of_birth as age,
        revenue.week_nubmer as week_nubmer,
        null as value_segment,
        null as devices,
        null as active_devices,
        null as mobile_type,
        null as brand_name,
        null as os_name,
        null as os_vendor,
        null as customer,
        null as min_revenue,
        null as max_revenue,
        sum(revenue.revenue) as revenue_generated
    from TELECOM_ANALYSIS.DBT_CURATED.CRM as crm
    inner join TELECOM_ANALYSIS.DBT_CURATED.REVENUE as revenue on crm.msisdn = revenue.msisdn
    group by age,week_nubmer 
    order by age,week_nubmer
), e_overall as (
    select
        'revenue_overall_by_age_of_value_segment' as "desc",
        null as gender,
        year(current_timestamp)-crm.year_of_birth as age,
        null as week_nubmer,
        crm.value_segment as value_segment,
        null as devices,
        null as active_devices,
        null as mobile_type,
        null as brand_name,
        null as os_name,
        null as os_vendor,
        null as customer,
        null as min_revenue,
        null as max_revenue,
        sum(revenue.revenue) as revenue_generated
    from TELECOM_ANALYSIS.DBT_CURATED.CRM as crm
    inner join TELECOM_ANALYSIS.DBT_CURATED.REVENUE as revenue on crm.msisdn = revenue.msisdn
    group by age,value_segment
    order by revenue_generated desc
), e_weekwise as (
    select
        'revenue_weekwise_by_age_of_value_segment' as "desc",
        null as gender,
        year(current_timestamp)-crm.year_of_birth as age,
        revenue.week_nubmer as week_nubmer,
        crm.value_segment as value_segment,
        null as devices,
        null as active_devices,
        null as mobile_type,
        null as brand_name,
        null as os_name,
        null as os_vendor,
        null as customer,
        null as min_revenue,
        null as max_revenue,
        sum(revenue.revenue) as revenue_generated
    from TELECOM_ANALYSIS.DBT_CURATED.CRM as crm
    inner join TELECOM_ANALYSIS.DBT_CURATED.REVENUE as revenue on crm.msisdn = revenue.msisdn
    group by age,week_nubmer,value_segment
    order by age,week_nubmer,value_segment
), f_overall as (
    select
        'revenue_overall_by_mobile_type' as "desc",
        null as gender,
        null as age,
        null as week_nubmer,
        null as value_segment,
        null as devices,
        null as active_devices,
        crm.mobile_type as mobile_type,
        null as brand_name,
        null as os_name,
        null as os_vendor,
        null as customer,
        null as min_revenue,
        null as max_revenue,
        sum(revenue.revenue) as revenue_generated
    from TELECOM_ANALYSIS.DBT_CURATED.CRM as crm
    inner join TELECOM_ANALYSIS.DBT_CURATED.REVENUE as revenue on crm.msisdn = revenue.msisdn
    group by mobile_type
    order by mobile_type,revenue_generated
), f_weekwise as (
    select
        'revenue_weekwise_by_mobile_type' as "desc",
        null as gender,
        null as age,
        revenue.week_nubmer as week_nubmer,
        null as value_segment,
        null as devices,
        null as active_devices,
        crm.mobile_type as mobile_type,
        null as brand_name,
        null as os_name,
        null as os_vendor,
        null as customer,
        null as min_revenue,
        null as max_revenue,
        sum(revenue.revenue) as revenue_generated
    from TELECOM_ANALYSIS.DBT_CURATED.CRM as crm
    inner join TELECOM_ANALYSIS.DBT_CURATED.REVENUE as revenue on crm.msisdn = revenue.msisdn
    group by mobile_type,week_nubmer
    order by mobile_type,week_nubmer
), g_overall as (
    select
        'revenue_overall_by_brand' as "desc",
        null as gender,
        null as age,
        null as week_nubmer,
        null as value_segment,
        null as devices,
        null as active_devices,
        null as mobile_type,
        devices.brand_name as brand_name,
        null as os_name,
        null as os_vendor,
        null as customer,
        null as min_revenue,
        null as max_revenue,
        sum(revenue.revenue) as revenue_generated
    from TELECOM_ANALYSIS.DBT_CURATED.DEVICES as devices
    inner join TELECOM_ANALYSIS.DBT_CURATED.REVENUE as revenue on devices.msisdn = revenue.msisdn
    group by brand_name
    order by revenue_generated desc
), g_weekwise as (
    select
        'revenue_weekwise_by_brand' as "desc",
        null as gender,
        null as age,
        revenue.week_nubmer as week_nubmer,
        null as value_segment,
        null as devices,
        null as active_devices,
        null as mobile_type,
        devices.brand_name as brand_name,
        null as os_name,
        null as os_vendor,
        null as customer,
        null as min_revenue,
        null as max_revenue,
        sum(revenue.revenue) as revenue_generated
    from TELECOM_ANALYSIS.DBT_CURATED.DEVICES as devices
    inner join TELECOM_ANALYSIS.DBT_CURATED.REVENUE as revenue on devices.msisdn = revenue.msisdn
    group by brand_name,week_nubmer
    order by brand_name,week_nubmer
), h_overall as (
    select
        'revenue_overall_by_os_name' as "desc",
        null as gender,
        null as age,
        null as week_nubmer,
        null as value_segment,
        null as devices,
        null as active_devices,
        null as mobile_type,
        null as brand_name,
        devices.os_name as os_name,
        null as os_vendor,
        null as customer,
        null as min_revenue,
        null as max_revenue,
        sum(revenue.revenue) as revenue_generated
    from TELECOM_ANALYSIS.DBT_CURATED.DEVICES as devices
    inner join TELECOM_ANALYSIS.DBT_CURATED.REVENUE as revenue on devices.msisdn = revenue.msisdn
    group by os_name
    order by revenue_generated desc
), h_weekwise as (
    select
        'revenue_weekwise_by_os_name' as "desc",
        null as gender,
        null as age,
        revenue.week_nubmer as week_nubmer,
        null as value_segment,
        null as devices,
        null as active_devices,
        null as mobile_type,
        null as brand_name,
        devices.os_name as os_name,
        null as os_vendor,
        null as customer,
        null as min_revenue,
        null as max_revenue,
        sum(revenue.revenue) as revenue_generated
    from TELECOM_ANALYSIS.DBT_CURATED.DEVICES as devices
    inner join TELECOM_ANALYSIS.DBT_CURATED.REVENUE as revenue on devices.msisdn = revenue.msisdn
    group by os_name,week_nubmer
    order by os_name,week_nubmer
), i_overall as (
    select
        'revenue_overall_by_os_vendor' as "desc",
        null as gender,
        null as age,
        null as week_nubmer,
        null as value_segment,
        null as devices,
        null as active_devices,
        null as mobile_type,
        null as brand_name,
        null as os_name,
        devices.os_vendor as os_vendor,
        null as customer,
        null as min_revenue,
        null as max_revenue,
        sum(revenue.revenue) as revenue_generated
    from TELECOM_ANALYSIS.DBT_CURATED.DEVICES as devices
    inner join TELECOM_ANALYSIS.DBT_CURATED.REVENUE as revenue on devices.msisdn = revenue.msisdn
    group by os_vendor
    order by revenue_generated desc
), i_weekwise as (
    select
        'revenue_weekwise_by_os_vendor' as "desc",
        null as gender,
        null as age,
        revenue.week_nubmer as week_nubmer,
        null as value_segment,
        null as devices,
        null as active_devices,
        null as mobile_type,
        null as brand_name,
        null as os_name,
        devices.os_vendor as os_vendor,
        null as customer,
        null as min_revenue,
        null as max_revenue,
        sum(revenue.revenue) as revenue_generated
    from TELECOM_ANALYSIS.DBT_CURATED.DEVICES as devices
    inner join TELECOM_ANALYSIS.DBT_CURATED.REVENUE as revenue on devices.msisdn = revenue.msisdn
    group by os_vendor,week_nubmer
    order by os_vendor,week_nubmer
), j_overall as (
    select
        'total_customer_overall_by_os_name' as "desc",
        null as gender,
        null as age,
        null as week_nubmer,
        null as value_segment,
        null as devices,
        null as active_devices,
        null as mobile_type,
        null as brand_name,
        devices.os_name as os_name,
        null as os_vendor,
        count(*) as customer,
        null as min_revenue,
        null as max_revenue,
        null as revenue_generated
    from TELECOM_ANALYSIS.DBT_CURATED.DEVICES as devices
    group by os_name
    order by customer desc
), j_weekwise as (
    select
        'total_customer_weekwise_by_os_name' as "desc",
        null as gender,
        null as age,
        revenue.week_nubmer as week_nubmer,
        null as value_segment,
        null as devices,
        null as active_devices,
        null as mobile_type,
        null as brand_name,
        devices.os_name as os_name,
        null as os_vendor,
        count(*) as customer,
        null as min_revenue,
        null as max_revenue,
        null as revenue_generated
    from TELECOM_ANALYSIS.DBT_CURATED.DEVICES as devices
    inner join TELECOM_ANALYSIS.DBT_CURATED.REVENUE as revenue on devices.msisdn = revenue.msisdn
    group by os_name, week_nubmer
    order by os_name, week_nubmer
), k_overall as (
    select
        'total_customer_overall_by_brand_name' as "desc",
        null as gender,
        null as age,
        null as week_nubmer,
        null as value_segment,
        null as devices,
        null as active_devices,
        null as mobile_type,
        devices.brand_name as brand_name,
        null as os_name,
        null as os_vendor,
        count(*) as customer,
        null as min_revenue,
        null as max_revenue,
        null as revenue_generated
    from TELECOM_ANALYSIS.DBT_CURATED.DEVICES as devices
    group by brand_name
    order by customer desc
), k_weekwise as (
    select
        'total_customer_weekwise_by_brand_name' as "desc",
        null as gender,
        null as age,
        revenue.week_nubmer as week_nubmer,
        null as value_segment,
        null as devices,
        null as active_devices,
        null as mobile_type,
        devices.brand_name as brand_name,
        null as os_name,
        null as os_vendor,
        count(*) as customer,
        null as min_revenue,
        null as max_revenue,
        null as revenue_generated
    from TELECOM_ANALYSIS.DBT_CURATED.DEVICES as devices
    inner join TELECOM_ANALYSIS.DBT_CURATED.REVENUE as revenue on devices.msisdn = revenue.msisdn
    group by brand_name,week_nubmer
    order by brand_name,week_nubmer
), l_overall as (
    select
        'total_customer_overall_by_mobile_type' as "desc",
        null as gender,
        null as age,
        null as week_nubmer,
        null as value_segment,
        null as devices,
        null as active_devices,
        crm.mobile_type as mobile_type,
        null as brand_name,
        null as os_name,
        null as os_vendor,
        count(*) as customer,
        null as min_revenue,
        null as max_revenue,
        null as revenue_generated
    from TELECOM_ANALYSIS.DBT_CURATED.CRM as crm
    group by mobile_type
    order by customer desc
), l_weekwise as (
    select
        'total_customer_weekwise_by_mobile_type' as "desc",
        null as gender,
        null as age,
        revenue.week_nubmer as week_nubmer,
        null as value_segment,
        null as devices,
        null as active_devices,
        crm.mobile_type as mobile_type,
        null as brand_name,
        null as os_name,
        null as os_vendor,
        count(*) as customer,
        null as min_revenue,
        null as max_revenue,
        null as revenue_generated
    from TELECOM_ANALYSIS.DBT_CURATED.CRM as crm
    inner join TELECOM_ANALYSIS.DBT_CURATED.REVENUE revenue on crm.msisdn = revenue.msisdn
    group by mobile_type, week_nubmer
    order by mobile_type,week_nubmer
), m as (
    select
        'week_wise_low_high_revenue_by_brand_name' as "desc",
        null as gender,
        null as age,
        revenue.week_nubmer as week_nubmer,
        null as value_segment,
        null as devices,
        null as active_devices,
        null as mobile_type,
        devices.brand_name as brand_name,
        null as os_name,
        null as os_vendor,
        null as customer,
        min(revenue.revenue) as min_revenue,
        max(revenue.revenue) as max_revenue,
        null as revenue_generated
    from TELECOM_ANALYSIS.DBT_CURATED.DEVICES as devices
    inner join TELECOM_ANALYSIS.DBT_CURATED.REVENUE as revenue on devices.msisdn = revenue.msisdn
    group by brand_name,week_nubmer
    order by brand_name,week_nubmer
), n as (
    select
        'week_wise_low_high_revenue_by_os_name' as "desc",
        null as gender,
        null as age,
        revenue.week_nubmer as week_nubmer,
        null as value_segment,
        null as devices,
        null as active_devices,
        null as mobile_type,
        null as brand_name,
        devices.os_name as os_name,
        null as os_vendor,
        null as customer,
        min(revenue.revenue) as min_revenue,
        max(revenue.revenue) as max_revenue,
        null as revenue_generated
    from TELECOM_ANALYSIS.DBT_CURATED.DEVICES as devices
    inner join TELECOM_ANALYSIS.DBT_CURATED.REVENUE as revenue on devices.msisdn = revenue.msisdn
    group by os_name,week_nubmer
    order by os_name,week_nubmer
), o as (
    select
        'brand_name_distbn_by_age_20_30' as "desc",
        null as gender,
        null as age,
        null as week_nubmer,
        null as value_segment,
        null as devices,
        null as active_devices,
        null as mobile_type,
        devices.brand_name as brand_name,
        null as os_name,
        null as os_vendor,
        count(*) as customer,
        null as min_revenue,
        null as max_revenue,
        null as revenue_generated
    from TELECOM_ANALYSIS.DBT_CURATED.DEVICES as devices
    inner join TELECOM_ANALYSIS.DBT_CURATED.CRM as crm on devices.msisdn = crm.msisdn
    where (year(current_timestamp)-crm.year_of_birth between 20 and 30)
    group by brand_name
    order by customer desc
), p as (
    select
        'brand_name_distbn_by_age_20_30' as "desc",
        null as gender,
        null as age,
        null as week_nubmer,
        null as value_segment,
        null as devices,
        null as active_devices,
        null as mobile_type,
        null as brand_name,
        devices.os_name as os_name,
        null as os_vendor,
        count(*) as customer,
        null as min_revenue,
        null as max_revenue,
        null as revenue_generated
    from TELECOM_ANALYSIS.DBT_CURATED.DEVICES as devices
    inner join TELECOM_ANALYSIS.DBT_CURATED.CRM as crm on devices.msisdn = crm.msisdn
    where (year(current_timestamp)-crm.year_of_birth between 20 and 30)
    group by os_name
    order by customer desc
), final_table as (
    select * from a_weekwise union all 
    select * from a_weekwise_active union all
    select * from b_weekwise union all 
    select * from b_weekwise_active union all
    select * from c_overall union all 
    select * from c_weekwise union all 
    select * from d_overall union all 
    select * from d_weekwise union all 
    select * from e_overall union all 
    select * from e_weekwise union all 
    select * from f_overall union all 
    select * from f_weekwise union all 
    select * from g_overall union all 
    select * from g_weekwise union all 
    select * from h_overall union all 
    select * from h_weekwise union all
    select * from i_overall union all 
    select * from i_weekwise union all 
    select * from j_overall union all 
    select * from j_weekwise union all 
    select * from k_overall union all 
    select * from k_weekwise union all 
    select * from m union all
    select * from n union all
    select * from o union all
    select * from p
) 
select * from final_table;
