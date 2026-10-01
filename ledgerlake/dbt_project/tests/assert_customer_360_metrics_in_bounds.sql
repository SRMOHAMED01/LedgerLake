select *
from {{ ref('customer_360') }}
where age < 0
   or total_transactions < 0
   or average_transaction < 0
   or total_spent < 0
   or active_loans_count < 0
   or credit_score < 300
   or credit_score > 850
