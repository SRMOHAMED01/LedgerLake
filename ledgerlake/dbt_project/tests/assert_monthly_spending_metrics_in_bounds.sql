select *
from {{ ref('customer_monthly_spending') }}
where total_spending < 0
   or number_transactions < 1
