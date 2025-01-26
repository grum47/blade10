drop table if exists {{ params.pg_clean_schema }}.{{ task.sql[0].split('_')[0] }};
create table if not exists {{ params.pg_clean_schema }}.{{ task.sql[0].split('_')[0] }} as
select 	id::int as area_id
        , parent_id::int as area_parent_id
        , "name" as area_name
        , '{{ params.process_ds }}'::date as md_ins_date 
        , '{{ params.process_ds }}'::date as md_upd_date 
        , 1 as md_is_activ 
        , 0 as md_is_delete 
from    {{ params.pg_raw_schema }}.areas a
order by 1;