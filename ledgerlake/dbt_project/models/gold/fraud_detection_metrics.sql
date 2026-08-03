-- fraud_detection_metrics.sql — Métriques de fraude par client (Palier C.2)
-- À IMPLÉMENTER :
--   customer_id, number_transactions, suspicious_operations, fraud_score
-- suspicious_operations : règle simple au début (ex: montant > 3x la moyenne
-- du client, ou transactions dans plusieurs pays le même jour).

select
    customer_id,
    count(*) as number_transactions,
    1 as placeholder -- TODO: suspicious_operations, fraud_score
from {{ source('silver', 'transactions') }}
group by customer_id
