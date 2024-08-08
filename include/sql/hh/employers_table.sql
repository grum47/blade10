drop table if exists {{ params.pg_clean_schema }}.{{ task.sql[0].split('_')[0] }};
create table if not exists {{ params.pg_clean_schema }}.{{ task.sql[0].split('_')[0] }} as
select 	id as employer_id
		, "name" as employer_name
		, "type" as employer_type
		, site_url 
		, vacancies_url
		, area_id as city_id
		, open_vacancies
		, current_date as md_ins_date 
        , current_date as md_upd_date 
        , 1 as md_is_activ
        , 0 as md_is_delete 
from 	{{ params.pg_raw_schema }}.employers
order by 1;