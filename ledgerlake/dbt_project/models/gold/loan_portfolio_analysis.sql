-- loan_portfolio_analysis.sql — Analyse du portefeuille de crédits (Palier C.2)
-- À IMPLÉMENTER :
--   loan_id, customer_id, amount, default_probability, risk_category
-- default_probability : au début une valeur simulée/règle simple,
-- plus tard (Palier H) peut venir du modèle XGBoost de CreditPulse.

select
    loan_id,
    1 as placeholder -- TODO
from {{ source('silver', 'loans') }}
