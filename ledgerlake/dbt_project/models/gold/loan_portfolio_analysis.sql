-- loan_portfolio_analysis.sql — Analyse du portefeuille de crédits (Palier C.2)
-- Colonnes attendues :
--   loan_id, customer_id, loan_amount, default_probability, risk_category

with latest_loans as (
    select * from (
        select *, row_number() over(partition by loan_id order by loan_updated_at desc) as rn
        from {{ ref('stg_loans') }}
    )
    where rn = 1
)

select
    loan_id,
    customer_id,
    loan_amount,
    duration_months,
    loan_status,
    
    -- Basic heuristic for default probability (Palier C)
    -- Later replaced by ML model from CreditPulse (Palier H)
    case
        when loan_status = 'DEFAULTED' then 1.00
        when loan_status = 'REJECTED' then 0.90
        when duration_months > 60 and loan_amount > 200000 then 0.45
        when duration_months > 120 then 0.30
        else 0.05
    end as default_probability,
    
    case
        when loan_status = 'DEFAULTED' then 'Critical'
        when duration_months > 60 and loan_amount > 200000 then 'High'
        when duration_months > 120 then 'Medium'
        else 'Low'
    end as risk_category

from latest_loans
