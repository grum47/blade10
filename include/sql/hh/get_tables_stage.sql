select  table_name 
from 	information_schema.tables
where 	table_schema = '{{ params.pg_raw_schema }}'
order by 1;