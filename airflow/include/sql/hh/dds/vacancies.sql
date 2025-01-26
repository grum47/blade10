CREATE TABLE IF NOT EXISTS blade_dds.vacancies (
    dwh_id serial PRIMARY KEY, 
    vacancy_id double precision, 
    city_id double precision, 
    country_id double precision, 
    professional_role_id double precision, 
    employer_id double precision, 
    metro_line_id integer, 
    metro_station_id integer, 
    salary_from double precision, 
    salary_to double precision, 
    vacancy_name text, 
    vacancy_type_id text, 
    experience_id text, 
    schedule_id text, 
    employmenr_id text, 
    working_days_id text, 
    working_time_intervals_id text, 
    working_time_modes_id text, 
    language_id text, 
    language_level_id text, 
    approved boolean, 
    key_skills _text, 
    md_ins_date date, 
    md_upd_date date, 
    md_is_activ integer, 
    md_is_delete integer, 
    md_dwh_is_activ int4 not null, 
    md_dwh_date_from date not null, 
    md_dwh_date_to date not null, 
    md_dwh_status char(1) not null
    );

-- (1)
insert into blade_dds.vacancies (
vacancy_id,
city_id, 
country_id,
professional_role_id,
employer_id,
metro_line_id,
metro_station_id,
salary_from,
salary_to,
vacancy_name,
vacancy_type_id,
experience_id,
schedule_id,
employmenr_id,
working_days_id,
working_time_intervals_id,
working_time_modes_id,
language_id,
language_level_id,
approved,
key_skills,
md_ins_date,
md_upd_date,
md_is_activ,
md_is_delete,
md_dwh_is_activ,
md_dwh_date_from,
md_dwh_date_to,
md_dwh_status)
select 		ods.vacancy_id 
			, ods.city_id 
			, ods.country_id 
			, ods.professional_role_id 
			, ods.employer_id 
			, ods.metro_line_id 
			, ods.metro_station_id 
			, ods.salary_from 
			, ods.salary_to 
			, ods.vacancy_name 
			, ods.vacancy_type_id 
			, ods.experience_id
			, ods.schedule_id 
			, ods.employmenr_id 
			, ods.working_days_id 
			, ods.working_time_intervals_id 
			, ods.working_time_modes_id 
			, ods.language_id 
			, ods.language_level_id 
			, ods.approved 
			, ods.key_skills 
			, ods.md_ins_date 
			, ods.md_upd_date 
			, ods.md_is_activ 
			, ods.md_is_delete 
			, 1 as md_dwh_is_activ
			, (ods.md_ins_date - '1 day'::interval)::date as md_dwh_date_from 
			, '9999-12-31'::date as md_dwh_date_to 
			, 'I' as md_dwh_status
from 		blade10.blade_ods.vacancies ods
full join	blade10.blade_dds.vacancies dds
on			ods.vacancy_id = dds.vacancy_id
where 		1=1 
and 		dds.dwh_id is null;


--(2) обновляем метаданные в строках, в которыхе есть изменения 
update 	blade_dds.vacancies
set md_dwh_date_to = sbq.md_dwh_date_to,
	md_dwh_is_activ = sbq.md_dwh_is_activ
from 
(
select 		dds.dwh_id 
			, ods.md_ins_date 
			, (ods.md_ins_date - '1 day'::interval)::date as md_dwh_date_to 
			, 0 as md_dwh_is_activ 
from 		blade10.blade_ods.vacancies ods
full join	blade10.blade_dds.vacancies dds
on			ods.vacancy_id = dds.vacancy_id
where 		1=1
and 		dds.md_dwh_is_activ = 1
and			dds.md_dwh_status != 'D' 	
and 		ods.vacancy_id is not null
and 		dds.dwh_id is not null
and  		md5(
			'' ||
			coalesce(ods.city_id::text, '') ||
			coalesce(ods.country_id::text, '') ||
			coalesce(ods.professional_role_id::text, '') ||
			coalesce(ods.employer_id::text, '') ||
			coalesce(ods.metro_line_id::text, '') ||
			coalesce(ods.metro_station_id::text, '') ||
			coalesce(ods.salary_from::text, '') ||
			coalesce(ods.salary_to::text, '') ||
			coalesce(ods.vacancy_name::text, '') ||
			coalesce(ods.vacancy_type_id::text, '')  ||
			coalesce(ods.experience_id::text, '') ||
			coalesce(ods.schedule_id::text, '') ||
			coalesce(ods.employmenr_id::text, '') ||
			coalesce(ods.working_days_id::text, '') ||
			coalesce(ods.working_time_intervals_id::text, '') ||
			coalesce(ods.working_time_modes_id::text, '') ||
			coalesce(ods.language_id::text, '') ||
			coalesce(ods.language_level_id::text, '') ||
			coalesce(ods.approved::text, '') ||
			coalesce(ods.key_skills::text, '')
			) != md5(
					'' ||
					coalesce(dds.city_id::text, '') ||
					coalesce(dds.country_id::text, '') ||
					coalesce(dds.professional_role_id::text, '') ||
					coalesce(dds.employer_id::text, '') ||
					coalesce(dds.metro_line_id::text, '') ||
					coalesce(dds.metro_station_id::text, '') ||
					coalesce(dds.salary_from::text, '') ||
					coalesce(dds.salary_to::text, '') ||
					coalesce(dds.vacancy_name::text, '') ||
					coalesce(dds.vacancy_type_id::text, '') ||
					coalesce(dds.experience_id::text, '') ||
					coalesce(dds.schedule_id::text, '') ||
					coalesce(dds.employmenr_id::text, '') ||
					coalesce(dds.working_days_id::text, '') ||
					coalesce(dds.working_time_intervals_id::text, '') ||
					coalesce(dds.working_time_modes_id::text, '') ||
					coalesce(dds.language_id::text, '') ||
					coalesce(dds.language_level_id::text, '') ||
					coalesce(dds.approved::text, '') ||
					coalesce(dds.key_skills::text, '')
					)
) as sbq
WHERE blade_dds.vacancies.dwh_id=sbq.dwh_id;

-- (3)  
insert into blade10.blade_dds.vacancies (
vacancy_id,
city_id, 
country_id,
professional_role_id,
employer_id,
metro_line_id,
metro_station_id,
salary_from,
salary_to,
vacancy_name,
vacancy_type_id,
experience_id,
schedule_id,
employmenr_id,
working_days_id,
working_time_intervals_id,
working_time_modes_id,
language_id,
language_level_id,
approved,
key_skills,
md_ins_date,
md_upd_date,
md_is_activ,
md_is_delete,
md_dwh_is_activ,
md_dwh_date_from,
md_dwh_date_to,
md_dwh_status)
select 		ods.vacancy_id 
			, ods.city_id 
			, ods.country_id 
			, ods.professional_role_id 
			, ods.employer_id 
			, ods.metro_line_id 
			, ods.metro_station_id 
			, ods.salary_from 
			, ods.salary_to 
			, ods.vacancy_name 
			, ods.vacancy_type_id 
			, ods.experience_id
			, ods.schedule_id 
			, ods.employmenr_id 
			, ods.working_days_id 
			, ods.working_time_intervals_id 
			, ods.working_time_modes_id 
			, ods.language_id 
			, ods.language_level_id 
			, ods.approved 
			, ods.key_skills 
			, ods.md_ins_date 
			, ods.md_upd_date 
			, ods.md_is_activ 
			, ods.md_is_delete 
			, 1 as md_dwh_is_activ 
			, ods.md_ins_date as md_dwh_date_from 
			, '9999-12-31'::date as md_dwh_date_to 
			, 'U' as md_dwh_status 
	from 		blade10.blade_ods.vacancies ods
	full join	blade10.blade_dds.vacancies dds
	on			ods.vacancy_id = dds.vacancy_id
	where 		1=1
	and 		ods.vacancy_id is not null
	and 		dds.md_dwh_is_activ = 0;

-- костыль чтобы не переполнялась таблица
delete from blade10.blade_dds.vacancies 
where dwh_id not in(
select dwh_id_not_del from
(select  vacancy_id 
		, md_dwh_is_activ 
		, max(dwh_id) as dwh_id_not_del
from 	blade10.blade_dds.vacancies v 
group by 1,2) as foo
);