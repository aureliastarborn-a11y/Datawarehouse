with current_dim as (
    select * from dim_customer
)

select
    customer_key,
    customer_id,
    first_name,
    last_name,
    email,
    customer_tier,
    city,
    state,
    country,
    effective_start_date,
    effective_end_date,
    is_current
from current_dim
