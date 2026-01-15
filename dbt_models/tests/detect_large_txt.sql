select *
from {{ ref('fct_transactions') }}
where amount > 100000
