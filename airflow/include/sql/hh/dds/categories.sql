CREATE TABLE IF NOT EXISTS blade_dds.categories (
    dwh_id serial PRIMARY KEY, 
    category_id bigint, 
    category_name text,
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
insert into blade_dds.categories (
category_id, category_name, 
md_ins_date, md_upd_date, md_is_activ, md_is_delete, 
md_dwh_is_activ, md_dwh_date_from, md_dwh_date_to, md_dwh_status
)
select 		ods.category_id
			, ods.category_name 
			, ods.md_ins_date 
			, ods.md_upd_date 
			, ods.md_is_activ 
			, ods.md_is_delete 
			, 1 as md_dwh_is_activ
			, (ods.md_ins_date - '1 day'::interval)::date as md_dwh_date_from 
			, '9999-12-31'::date as md_dwh_date_to 
			, 'I' as md_dwh_status
from 		blade10.blade_ods.categories ods
full join	blade10.blade_dds.categories dds
on			ods.category_id::int = dds.category_id::int
where 		1=1 
and 		dds.dwh_id is null;

-- (2) update old rows
update 	blade_dds.categories
set md_dwh_date_to = sbq.md_dwh_date_to,
	md_dwh_is_activ = sbq.md_dwh_is_activ
from 
(
select 		dds.dwh_id 
			, ods.md_ins_date 
			, (ods.md_ins_date - '1 day'::interval)::date as md_dwh_date_to 
			, 0 as md_dwh_is_activ
from 		blade_ods.categories ods
full join	blade_dds.categories dds
on			ods.category_id::int = dds.category_id::int
where 		1=1
and 		dds.md_dwh_is_activ = 1
and			dds.md_dwh_status != 'D' 	
and 		ods.category_id is not null
and 		dds.dwh_id is not null
and  		md5(
			'' ||
			coalesce(ods.category_id::text, '') ||
			coalesce(ods.category_name::text, '')
			) != md5(
					'' ||
					coalesce(dds.category_id::text, '') ||
					coalesce(dds.category_name::text, '')
					)
) as sbq
WHERE blade_dds.categories.dwh_id::int = sbq.dwh_id::int;

-- (3) new old rows 
insert into blade_dds.categories (
category_id, category_name,
md_ins_date,
md_upd_date,
md_is_activ,
md_is_delete,
md_dwh_is_activ,
md_dwh_date_from,
md_dwh_date_to,
md_dwh_status)
select 		ods.category_id 
			, ods.category_name 
			, ods.md_ins_date 
			, ods.md_upd_date 
			, ods.md_is_activ 
			, ods.md_is_delete 
			, 1 as md_dwh_is_activ 
			, ods.md_ins_date as md_dwh_date_from 
			, '9999-12-31'::date as md_dwh_date_to 
			, 'U' as md_dwh_status 
	from 		blade_ods.categories ods
	full join	blade_dds.categories dds
	on			ods.category_id::int = dds.category_id::int
	where 		1=1
	and 		dds.md_dwh_is_activ = 0;