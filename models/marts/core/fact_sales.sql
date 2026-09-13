with stg_orders as (
    select * from {{ ref('stg_orders') }}
),
dim_customer as (
    select * from {{ ref('dim_customer') }}
),
dim_product as (
    select * from {{ ref('dim_product') }}
)

select
    row_number() over (order by o.order_id, o.order_line_number) as sales_fact_key,
    o.order_id,
    o.order_line_number,
    c.customer_key,
    p.product_key,
    cast(strftime(o.order_timestamp, '%Y%m%d') as integer) as order_date_key,
    o.quantity,
    o.unit_price,
    round(o.quantity * o.unit_price, 2) as gross_amount,
    round(o.discount_amount, 2) as discount_amount,
    round(o.tax_amount, 2) as tax_amount,
    round((o.quantity * o.unit_price) - o.discount_amount + o.tax_amount, 2) as net_amount,
    round(o.quantity * p.cost_price, 2) as cost_amount,
    round(((o.quantity * o.unit_price) - o.discount_amount + o.tax_amount) - (o.quantity * p.cost_price), 2) as profit_amount,
    round(
        (
            ((o.quantity * o.unit_price) - o.discount_amount + o.tax_amount) - (o.quantity * p.cost_price)
        ) / nullif(((o.quantity * o.unit_price) - o.discount_amount + o.tax_amount), 0) * 100,
        2
    ) as profit_margin_pct
from stg_orders o
join dim_customer c
  on o.customer_id = c.customer_id
 and o.order_timestamp >= c.effective_start_date
 and (c.effective_end_date is null or o.order_timestamp < c.effective_end_date)
join dim_product p
  on o.product_id = p.product_id
