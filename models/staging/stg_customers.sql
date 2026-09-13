with raw_source as (
    select * from {{ source('raw_data', 'stg_customers') }}
),

cleaned as (
    select
        trim(raw_customer_id) as customer_id,
        trim(first_name) as first_name,
        trim(last_name) as last_name,
        lower(trim(email)) as email,
        coalesce(nullif(trim(customer_tier), ''), 'Standard') as customer_tier,
        trim(city) as city,
        trim(state) as state,
        trim(country) as country,
        coalesce(updated_at, current_timestamp) as updated_at,
        row_number() over (
            partition by raw_customer_id 
            order by updated_at desc
        ) as dedupe_row_num
    from raw_source
    where raw_customer_id is not null and trim(raw_customer_id) <> ''
)

select
    customer_id,
    first_name,
    last_name,
    email,
    customer_tier,
    city,
    state,
    country,
    updated_at
from cleaned
where dedupe_row_num = 1
