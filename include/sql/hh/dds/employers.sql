CREATE TABLE IF NOT EXISTS blade_dds.employers (
    dwh_id serial PRIMARY KEY, 
    employer_id bigint, 
    employer_name text, 
    employer_type text, 
    site_url text, 
    vacancies_url text, 
    city_id text, 
    open_vacancies text, 
    md_ins_date date, 
    md_upd_date date, 
    md_is_activ integer, 
    md_is_delete integer, 
    md_dwh_is_activ int4 not null, 
    md_dwh_date_from date not null, 
    md_dwh_date_to date not null, 
    md_dwh_status char(1) not null
    );

-- (1) new rows
insert into blade_dds.employers (
employer_id, employer_name, employer_type, site_url, vacancies_url, city_id, open_vacancies,
md_ins_date, md_upd_date, md_is_activ, md_is_delete, 
md_dwh_is_activ, md_dwh_date_from, md_dwh_date_to, md_dwh_status
)
select 		ods.employer_id 
			, ods.employer_name
            , ods.employer_type
            , ods.site_url
            , ods.vacancies_url
            , ods.city_id
            , ods.open_vacancies 
			, ods.md_ins_date 
			, ods.md_upd_date 
			, ods.md_is_activ 
			, ods.md_is_delete 
			, 1 as md_dwh_is_activ
			, (ods.md_ins_date - '1 day'::interval)::date as md_dwh_date_from 
			, '9999-12-31'::date as md_dwh_date_to 
			, 'I' as md_dwh_status
from 		blade10.blade_ods.employers ods
full join	blade10.blade_dds.employers dds
on			ods.city_id = dds.city_id
where 		1=1 
and 		dds.dwh_id is null;

-- (2) update old rows
update 	blade_dds.employers
set md_dwh_date_to = sbq.md_dwh_date_to,
	md_dwh_is_activ = sbq.md_dwh_is_activ
from 
(
select 		dds.dwh_id 
			, ods.md_ins_date 
			, (ods.md_ins_date - '1 day'::interval)::date as md_dwh_date_to 
			, 0 as md_dwh_is_activ
from 		blade_ods.employers ods
full join	blade_dds.employers dds
on			ods.employer_id = dds.employer_id
where 		1=1
and 		dds.md_dwh_is_activ = 1
and			dds.md_dwh_status != 'D' 	
and 		ods.city_id is not null
and 		dds.dwh_id is not null
and  		md5(
			'' ||
			coalesce(ods.employer_id::text, '') ||
			coalesce(ods.employer_name::text, '') ||
            coalesce(ods.employer_type::text, '') ||
            coalesce(ods.site_url::text, '') ||
            coalesce(ods.city_id::text, '')
			) != md5(
					'' ||
					coalesce(dds.employer_id::text, '') ||
                    coalesce(dds.employer_name::text, '') ||
                    coalesce(dds.employer_type::text, '') ||
                    coalesce(dds.site_url::text, '') ||
                    coalesce(dds.city_id::text, '')
					)
) as sbq
WHERE blade_dds.employers.dwh_id=sbq.dwh_id;

-- (3) new old rows 
insert into blade_dds.employers (
    employer_id, employer_name, employer_type, site_url, vacancies_url, city_id, open_vacancies,
    md_ins_date, md_upd_date, md_is_activ, md_is_delete,
    md_dwh_is_activ, md_dwh_date_from, md_dwh_date_to, md_dwh_status
)
select 		ods.employer_id 
			, ods.employer_name
            , ods.employer_type
            , ods.site_url
            , ods.vacancies_url
            , ods.city_id
            , ods.open_vacancies  
			, ods.md_ins_date 
			, ods.md_upd_date 
			, ods.md_is_activ 
			, ods.md_is_delete 
			, 1 as md_dwh_is_activ 
			, ods.md_ins_date as md_dwh_date_from 
			, '9999-12-31'::date as md_dwh_date_to 
			, 'U' as md_dwh_status 
from 		blade_ods.employers ods
full join	blade_dds.employers dds
on			ods.employer_id = dds.employer_id
where 		1=1
and 		dds.md_dwh_is_activ = 0
and         ods.md_ins_date is not null;