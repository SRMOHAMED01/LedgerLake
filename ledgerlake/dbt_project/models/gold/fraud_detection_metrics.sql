-- fraud_detection_metrics.sql — Métriques de fraude par client (Palier C.2)
-- Colonnes attendues :
--   customer_id, number_transactions, suspicious_operations, fraud_score

with customer_stats as (
    select
        customer_id,
        count(transaction_id) as number_transactions,
        avg(amount) as avg_amount,
        count(distinct transaction_country) as distinct_countries
    from {{ ref('stg_transactions') }}
    group by customer_id
),
suspicious_tx as (
    select
        t.customer_id,
        sum(case when t.amount > (c.avg_amount * 3) then 1 else 0 end) as high_amount_tx_count
    from {{ ref('stg_transactions') }} t
    join customer_stats c on t.customer_id = c.customer_id
    group by t.customer_id
)

select
    c.customer_id,
    c.number_transactions,
    (s.high_amount_tx_count + (case when c.distinct_countries > 2 then 1 else 0 end)) as suspicious_operations,
    
    -- Basic Fraud Score out of 100
    least(100, (s.high_amount_tx_count * 20) + (case when c.distinct_countries > 2 then 30 else 0 end)) as fraud_score

from customer_stats c
left join suspicious_tx s on c.customer_id = s.customer_id
