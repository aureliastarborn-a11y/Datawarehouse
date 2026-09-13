with stg_products as (
    select * from {{ ref('stg_products') }}
)

select
    row_number() over (order by product_id) as product_key,
    product_id,
    product_name,
    category,
    subcategory,
    unit_price,
    cost_price,
    true as is_active,
    updated_at
from stg_products
