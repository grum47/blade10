drop table if exists {{ params.pg_clean_schema }}.{{ task.parameters[0] }};
create table if not exists {{ params.pg_clean_schema }}.{{ task.parameters[0] }} as 
select  id as "{{ task.parameters[0] }}_id"
        , "name" as "{{ task.parameters[0] }}_name"
        , '{{ params.process_ds }}'::date as md_ins_date 
        , '{{ params.process_ds }}'::date as md_upd_date 
        , 1 as md_is_activ
        , 0 as md_is_delete 
from 	{{ params.pg_raw_schema }}.{{ task.parameters[0] }}
order by 1;