with customer_first_purchase as (
    select
        f.customer_key,
        c.customer_id,
        min(d.full_date) as first_order_date,
        strftime(min(d.full_date), '%Y-%m') as cohort_month
    from {{ ref('fact_sales') }} f
    join {{ ref('dim_customer') }} c on f.customer_key = c.customer_key
    join {{ ref('dim_date') }} d on f.order_date_key = d.date_key
    group by f.customer_key, c.customer_id
),
cohort_size as (
    select
        cohort_month,
        count(distinct customer_id) as total_cohort_customers
    from customer_first_purchase
    group by cohort_month
),
customer_monthly_activity as (
    select
        cfp.cohort_month,
        strftime(d.full_date, '%Y-%m') as activity_month,
        f.customer_key,
        sum(f.net_amount) as monthly_net_spend
    from {{ ref('fact_sales') }} f
    join customer_first_purchase cfp on f.customer_key = cfp.customer_key
    join {{ ref('dim_date') }} d on f.order_date_key = d.date_key
    group by cfp.cohort_month, strftime(d.full_date, '%Y-%m'), f.customer_key
)
select
    act.cohort_month,
    cs.total_cohort_customers,
    act.activity_month,
    count(distinct act.customer_key) as active_retained_customers,
    round(count(distinct act.customer_key) * 100.0 / cs.total_cohort_customers, 2) as retention_rate_pct,
    sum(act.monthly_net_spend) as cohort_period_revenue,
    round(sum(act.monthly_net_spend) / cs.total_cohort_customers, 2) as avg_revenue_per_cohort_member
from customer_monthly_activity act
join cohort_size cs on act.cohort_month = cs.cohort_month
group by act.cohort_month, cs.total_cohort_customers, act.activity_month
order by act.cohort_month, act.activity_month
