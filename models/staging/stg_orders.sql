with raw_source as (
    select * from {{ source('raw_data', 'stg_orders') }}
),

cleaned as (
    select
        trim(order_id) as order_id,
        order_line_number,
        trim(customer_id) as customer_id,
        trim(product_id) as product_id,
        order_timestamp,
        quantity,
        unit_price,
        coalesce(discount_amount, 0.00) as discount_amount,
        coalesce(tax_amount, 0.00) as tax_amount,
        row_number() over (
            partition by order_id, order_line_number 
            order by order_timestamp desc
        ) as dedupe_row_num
    from raw_source
    where order_id is not null and trim(order_id) <> ''
      and customer_id is not null and trim(customer_id) <> ''
      and product_id is not null and trim(product_id) <> ''
      and quantity > 0
)

select
    order_id,
    order_line_number,
    customer_id,
    product_id,
    order_timestamp,
    quantity,
    unit_price,
    discount_amount,
    tax_amount
from cleaned
where dedupe_row_num = 1
