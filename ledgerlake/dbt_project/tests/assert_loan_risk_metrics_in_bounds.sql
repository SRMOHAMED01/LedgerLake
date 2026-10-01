select *
from {{ ref('loan_portfolio_analysis') }}
where loan_amount <= 0
   or duration_months <= 0
   or default_probability < 0
   or default_probability > 1
