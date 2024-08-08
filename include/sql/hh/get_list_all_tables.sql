select  table_name 
from 	information_schema.tables
where 	table_schema = '{{ params.raw_schema }}'
and     table_name not in ('areas', 'vacancies', 'professional_roles', 'metro', 'employers')
order by table_name;