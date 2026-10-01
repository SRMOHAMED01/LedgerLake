-- customer_360.sql — Vue à 360° du client (Palier C.2)
-- Colonnes attendues :
--   customer_id, age, country, total_transactions, average_transaction,
--   credit_score, risk_level

with customers as (
    -- Take the latest customer record per customer_id
    select * from (
        select *, row_number() over(partition by customer_id order by customer_created_at desc) as rn
        from {{ ref('stg_customers') }}
    )
    where rn = 1
),
transactions_agg as (
    select
        customer_id,
        count(transaction_id) as total_transactions,
        avg(amount) as average_transaction,
        sum(amount) as total_spent
    from {{ ref('stg_transactions') }}
    group by customer_id
),
loans_agg as (
    select
        customer_id,
        count(distinct loan_id) as active_loans_count,
        sum(case when loan_status = 'DEFAULTED' then 1 else 0 end) as defaults_count
    from (
        select *, row_number() over(partition by loan_id order by loan_updated_at desc) as rn
        from {{ ref('stg_loans') }}
    )
    where rn = 1
    group by customer_id
)

select
    c.customer_id,
    c.age,
    c.country,
    c.country_region,
    c.customer_segment,
    coalesce(t.total_transactions, 0) as total_transactions,
    coalesce(t.average_transaction, 0.0) as average_transaction,
    coalesce(t.total_spent, 0.0) as total_spent,
    coalesce(l.active_loans_count, 0) as active_loans_count,
    
    -- Basic logic for credit_score and risk_level
    case
        when coalesce(l.defaults_count, 0) > 0 then 'High Risk'
        when coalesce(t.total_spent, 0) < 1000 then 'Medium Risk'
        else 'Low Risk'
    end as risk_level,
    
    case
        when coalesce(l.defaults_count, 0) > 0 then 300
        else least(850, 600 + cast(coalesce(t.total_transactions, 0) as integer) * 10)
    end as credit_score

from customers c
left join transactions_agg t on c.customer_id = t.customer_id
left join loans_agg l on c.customer_id = l.customer_id

