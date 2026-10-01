select *
from {{ ref('fraud_detection_metrics') }}
where number_transactions < 1
   or suspicious_operations < 0
   or fraud_score < 0
   or fraud_score > 100
