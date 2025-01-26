select distinct employer::bigint 
from {{ params.pg_raw_schema }}.vacancies v 
where employer is not null
union
select 1 as employer
order by 1;