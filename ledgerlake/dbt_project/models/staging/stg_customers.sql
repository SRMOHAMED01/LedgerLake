select
    customer_id,
    age,
    country,
    country_region,
    customer_segment,
    event_timestamp as customer_created_at
from {{ source('silver', 'customers') }}
-- In a real scenario, you'd want to get the latest record per customer (SCD Type 1) or keep history (SCD Type 2).
-- We'll assume the Silver table handles the deduplication and represents the latest state for our gold models.
