with raw_source as (
    select * from {{ source('raw_data', 'stg_products') }}
),

cleaned as (
    select
        trim(raw_product_id) as product_id,
        trim(product_name) as product_name,
        coalesce(nullif(trim(category), ''), 'Unassigned') as category,
        coalesce(nullif(trim(subcategory), ''), 'General') as subcategory,
        coalesce(unit_price, 0.00) as unit_price,
        coalesce(cost_price, 0.00) as cost_price,
        updated_at,
        row_number() over (
            partition by raw_product_id 
            order by updated_at desc
        ) as dedupe_row_num
    from raw_source
    where raw_product_id is not null and trim(raw_product_id) <> ''
)

select
    product_id,
    product_name,
    category,
    subcategory,
    unit_price,
    cost_price,
    updated_at
from cleaned
where dedupe_row_num = 1
