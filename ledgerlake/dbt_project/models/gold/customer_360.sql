-- customer_360.sql — Vue à 360° du client (Palier C.2)
-- À IMPLÉMENTER : joindre les tables Silver customers + transactions,
-- calculer total_transactions, average_transaction, ajouter credit_score
-- et risk_level (règles simples au début, ou jointure avec CreditPulse
-- plus tard - Palier H).
--
-- Colonnes attendues :
--   customer_id, age, country, total_transactions, average_transaction,
--   credit_score, risk_level

select
    customer_id,
    -- TODO: colonnes calculées
    1 as placeholder
from {{ source('silver', 'customers') }}
