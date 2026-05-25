-- Silver layer: cleaned and typed orders
-- Filters out cancelled orders, casts types, standardizes status casing

select
    order_id,
    customer_id,
    cast(order_date as date) as order_date,
    case
        when upper(status) in ('DELIVERED', 'SHIPPED', 'INVOICED') then 'COMPLETED'
        when upper(status) in ('APPROVED', 'CREATED', 'PROCESSING', 'UNAVAILABLE') then 'PENDING'
        else upper(status)
    end as status,
    cast(total_amount as numeric) as total_amount,
    created_at
from {{ ref('bronze_orders') }}
where upper(status) != 'CANCELED'