with date_spine as (
    select
        cast(d as date) as full_date
    from generate_series(date '2024-01-01', date '2026-12-31', interval '1 day') as s(d)
)

select
    cast(strftime(full_date, '%Y%m%d') as integer) as date_key,
    full_date,
    strftime(full_date, '%A') as day_name,
    extract(dow from full_date) + 1 as day_of_week,
    extract(day from full_date) as day_of_month,
    extract(month from full_date) as month,
    strftime(full_date, '%B') as month_name,
    extract(quarter from full_date) as quarter,
    extract(year from full_date) as year,
    case when extract(dow from full_date) in (0, 6) then true else false end as is_weekend
from date_spine
