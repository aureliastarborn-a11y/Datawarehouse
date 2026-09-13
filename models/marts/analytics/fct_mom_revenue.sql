with monthly_sales as (
    select
        d.year,
        d.month,
        d.month_name,
        count(distinct f.order_id) as total_orders,
        count(f.sales_fact_key) as total_order_items,
        sum(f.quantity) as total_units_sold,
        sum(f.gross_amount) as total_gross_revenue,
        sum(f.discount_amount) as total_discounts,
        sum(f.net_amount) as total_net_revenue,
        sum(f.cost_amount) as total_cost,
        sum(f.profit_amount) as total_profit,
        round((sum(f.profit_amount) / nullif(sum(f.net_amount), 0)) * 100, 2) as overall_profit_margin_pct
    from {{ ref('fact_sales') }} f
    join {{ ref('dim_date') }} d on f.order_date_key = d.date_key
    group by d.year, d.month, d.month_name
)
select
    year,
    month,
    month_name,
    total_orders,
    total_units_sold,
    total_gross_revenue,
    total_discounts,
    total_net_revenue,
    lag(total_net_revenue) over (order by year, month) as prior_month_net_revenue,
    round(
        (total_net_revenue - lag(total_net_revenue) over (order by year, month)) 
        / nullif(lag(total_net_revenue) over (order by year, month), 0) * 100,
        2
    ) as mom_net_revenue_growth_pct,
    total_profit,
    overall_profit_margin_pct
from monthly_sales
order by year, month
