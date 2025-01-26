drop table if exists {{ params.pg_clean_schema }}.{{ task.sql[0].split('_')[0] }};
create table if not exists {{ params.pg_clean_schema }}.{{ task.sql[0].split('_')[0] }} as
with SBQ as
(
	select  v.id as vacancy_id
			, v.area as city_id
			, a.parent_id as country_id
			, v.professional_roles as professional_role_id
			, v.employer as employer_id
			, split_part(v.metro::text, '.', 1)::int as metro_line_id
			, split_part(v.metro::text, '.', 2)::int as metro_station_id
			, v.salary_from 
			, v.salary_to 
			, v."name" as vacancy_name
			, v."type" as vacancy_type_id
			, v.experience as experience_id
			, v.schedule as schedule_id
			, v.employment as employmenr_id
			, v.working_days as working_days_id
			, v.working_time_intervals as working_time_intervals_id
			, v.working_time_modes as working_time_modes_id
			, v.languages as language_id
			, v.languages_level as language_level_id
			, v.approved
			, string_to_array(replace(trim(both '{}' from v.key_skills), '"', ''), ',')::text[] as key_skills
			, initial_created_at::date as md_ins_date
			, published_at::date as md_upd_date
			, 1 as md_is_activ
			, 0 as md_is_delete 
			, row_number() over(partition by v.id) as rn
	from 	{{ params.pg_raw_schema }}.vacancies v 
	join	{{ params.pg_raw_schema }}.areas a 
	on		v."area" = a.id 
	order by 1
)
select  vacancy_id::bigint
			, city_id::int
			, country_id::int
			, professional_role_id::int
			, employer_id::bigint
			, metro_line_id::int
			, metro_station_id::int
			, salary_from 
			, salary_to 
			, vacancy_name
			, vacancy_type_id
			, experience_id
			, schedule_id
			, employmenr_id
			, working_days_id
			, working_time_intervals_id
			, working_time_modes_id
			, language_id
			, language_level_id
			, approved
			, key_skills
			, md_ins_date
			, md_upd_date
			, md_is_activ
			, md_is_delete 
from SBQ
where 	rn = 1;