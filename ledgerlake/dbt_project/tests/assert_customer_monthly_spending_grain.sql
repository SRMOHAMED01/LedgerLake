select
    customer_id,
    spending_month,
    count(*) as row_count
from {{ ref('customer_monthly_spending') }}
group by customer_id, spending_month
having count(*) > 1
