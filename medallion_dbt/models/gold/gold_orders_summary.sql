-- Gold layer: business-ready aggregation of completed/pending orders
-- One row per customer with total spend and order count

select
    customer_id,
    count(order_id)             as total_orders,
    sum(total_amount)           as total_revenue,
    min(order_date)             as first_order_date,
    max(order_date)             as last_order_date,
    round(avg(total_amount), 2) as avg_order_value
from {{ ref('silver_orders') }}
group by customer_id
order by total_revenue desc