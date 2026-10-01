select
    loan_id,
    customer_id,
    amount as loan_amount,
    duration_months,
    status as loan_status,
    event_timestamp as loan_updated_at
from {{ source('silver', 'loans') }}
-- In a real scenario, this would group by loan_id and take the most recent status.
-- We will handle that in the gold layer if necessary.