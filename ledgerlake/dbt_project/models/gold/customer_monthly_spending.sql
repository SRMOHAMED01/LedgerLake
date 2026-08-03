-- customer_monthly_spending.sql — Dépenses mensuelles par client (Palier C.2)

select
    customer_id,
    date_trunc('month', transaction_date) as month,
    sum(amount) as total_spending,
    count(*) as number_transactions
from {{ source('silver', 'transactions') }}
group by customer_id, date_trunc('month', transaction_date)
