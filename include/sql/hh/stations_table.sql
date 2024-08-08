drop table if exists {{ params.pg_clean_schema }}.{{ task.sql[0].split('_')[0] }};
create table if not exists {{ params.pg_clean_schema }}.{{ task.sql[0].split('_')[0] }} as
select 	distinct station_id
		, station_name
		, station_order
		, lat
		, lon
		, current_date as md_ins_date 
        , current_date as md_upd_date 
        , 1 as md_is_activ
        , 0 as md_is_delete 
from 	{{ params.pg_raw_schema }}.metro
order by 1;