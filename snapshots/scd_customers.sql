{% snapshot scd_customers %}

{{
    config(
      target_schema='dw',
      unique_key='customer_id',
      strategy='check',
      check_cols=['first_name', 'last_name', 'email', 'customer_tier', 'city', 'state', 'country'],
      updated_at='updated_at'
    )
}}

select * from {{ ref('stg_customers') }}

{% endsnapshot %}
