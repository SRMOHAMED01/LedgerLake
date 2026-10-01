select
    transaction_id,
    customer_id,
    amount,
    currency,
    type as transaction_type,
    merchant,
    country as transaction_country,
    country_region as transaction_region,
    timestamp as transaction_timestamp
from {{ source('silver', 'transactions') }}