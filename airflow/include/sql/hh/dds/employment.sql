CREATE TABLE IF NOT EXISTS blade_dds.employment (
    dwh_id serial PRIMARY KEY, 
    employment_id text, 
    employment_name text, 
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
insert into blade_dds.employment (
employment_id, employment_name, 
md_ins_date, md_upd_date, md_is_activ, md_is_delete, 
md_dwh_is_activ, md_dwh_date_from, md_dwh_date_to, md_dwh_status
)
select 		ods.employment_id 
			, ods.employment_name 
			, ods.md_ins_date 
			, ods.md_upd_date 
			, ods.md_is_activ 
			, ods.md_is_delete 
			, 1 as md_dwh_is_activ
			, (ods.md_ins_date - '1 day'::interval)::date as md_dwh_date_from 
			, '9999-12-31'::date as md_dwh_date_to 
			, 'I' as md_dwh_status
from 		blade10.blade_ods.employment ods
full join	blade10.blade_dds.employment dds
on			ods.employment_id = dds.employment_id
where 		1=1 
and 		dds.dwh_id is null;

-- (2) update old rows
update 	blade_dds.employment
set md_dwh_date_to = sbq.md_dwh_date_to,
	md_dwh_is_activ = sbq.md_dwh_is_activ
from 
(
select 		dds.dwh_id 
			, ods.md_ins_date 
			, (ods.md_ins_date - '1 day'::interval)::date as md_dwh_date_to 
			, 0 as md_dwh_is_activ
from 		blade_ods.employment ods
full join	blade_dds.employment dds
on			ods.employment_id = dds.employment_id
where 		1=1
and 		dds.md_dwh_is_activ = 1
and			dds.md_dwh_status != 'D' 	
and 		ods.employment_id is not null
and 		dds.dwh_id is not null
and  		md5(
			'' ||
			coalesce(ods.employment_id::text, '') ||
			coalesce(ods.employment_name::text, '')
			) != md5(
					'' ||
					coalesce(dds.employment_id::text, '') ||
					coalesce(dds.employment_name::text, '')
					)
) as sbq
WHERE blade_dds.employment.dwh_id=sbq.dwh_id;

-- (3) new old rows 
insert into blade_dds.employment (
    employment_id, employment_name,
    md_ins_date, md_upd_date, md_is_activ, md_is_delete,
    md_dwh_is_activ, md_dwh_date_from, md_dwh_date_to, md_dwh_status
)
select 		ods.employment_id 
			, ods.employment_name 
			, ods.md_ins_date 
			, ods.md_upd_date 
			, ods.md_is_activ 
			, ods.md_is_delete 
			, 1 as md_dwh_is_activ 
			, ods.md_ins_date as md_dwh_date_from 
			, '9999-12-31'::date as md_dwh_date_to 
			, 'U' as md_dwh_status 
from 		blade_ods.employment ods
full join	blade_dds.employment dds
on			ods.employment_id = dds.employment_id
where 		1=1
and 		dds.md_dwh_is_activ = 0;