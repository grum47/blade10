select distinct id 
from {{ params.pg_raw_schema }}.areas a 
where (parent_id is null or parent_id = '113') 
and id != '113' order by id;