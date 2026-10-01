-- customer_monthly_spending.sql — Dépenses mensuelles par client (Palier C.2)

select
    customer_id,
    date_trunc('month', transaction_timestamp) as spending_month,
    sum(amount) as total_spending,
    count(transaction_id) as number_transactions
from {{ ref('stg_transactions') }}
group by customer_id, date_trunc('month', transaction_timestamp)
