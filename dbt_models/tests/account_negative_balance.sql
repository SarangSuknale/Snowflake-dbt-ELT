
select 
       *
from {{ref('int_accounts')}}
where current_balance < 0 
  and account_type not in ('credit', 'loan')