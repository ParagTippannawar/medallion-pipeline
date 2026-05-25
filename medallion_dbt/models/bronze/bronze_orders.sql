-- Bronze layer: raw orders as-is from source table
-- No transformation, just a clean alias over the raw ingestion table

select
    order_id,
    customer_id,
    order_date,
    status,
    total_amount,
    created_at
from raw.orders