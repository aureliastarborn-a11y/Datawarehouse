with max_warehouse_date as (
    select max(full_date) as current_date 
    from {{ ref('dim_date') }} 
    where date_key in (select order_date_key from {{ ref('fact_sales') }})
),
customer_rfm_metrics as (
    select
        c.customer_id,
        concat(c.first_name, ' ', c.last_name) as customer_name,
        c.customer_tier,
        c.city,
        c.country,
        datediff('day', max(d.full_date), (select current_date from max_warehouse_date)) as recency_days,
        count(distinct f.order_id) as frequency_orders,
        sum(f.net_amount) as monetary_spend,
        sum(f.profit_amount) as total_customer_profit
    from {{ ref('fact_sales') }} f
    join {{ ref('dim_customer') }} c on f.customer_key = c.customer_key
    join {{ ref('dim_date') }} d on f.order_date_key = d.date_key
    where c.is_current = true
    group by c.customer_id, c.first_name, c.last_name, c.customer_tier, c.city, c.country
),
rfm_scores as (
    select
        *,
        ntile(4) over (order by recency_days asc) as r_score,
        ntile(4) over (order by frequency_orders desc) as f_score,
        ntile(4) over (order by monetary_spend desc) as m_score
    from customer_rfm_metrics
),
segmented_customers as (
    select
        *,
        case
            when r_score >= 3 and f_score >= 3 and m_score >= 3 then 'Champions (VIP)'
            when f_score >= 3 and m_score >= 2 then 'Loyal Customers'
            when r_score >= 3 and f_score <= 2 then 'Recent New Customers'
            when r_score <= 2 and f_score >= 2 then 'At Risk Customers'
            else 'Hibernating / Lost'
        end as rfm_segment
    from rfm_scores
)
select
    rfm_segment,
    count(customer_id) as total_customers,
    round(avg(recency_days), 1) as avg_days_since_last_order,
    round(avg(frequency_orders), 1) as avg_orders_per_customer,
    round(avg(monetary_spend), 2) as avg_customer_spend,
    round(sum(monetary_spend), 2) as total_segment_revenue,
    round(sum(total_customer_profit), 2) as total_segment_profit
from segmented_customers
group by rfm_segment
order by total_segment_revenue desc
