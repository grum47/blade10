CREATE TABLE IF NOT EXISTS blade_dds.messaging_status (
    dwh_id serial PRIMARY KEY, 
    messaging_status_id text, 
    messaging_status_name text, 
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
insert into blade_dds.messaging_status (
messaging_status_id, messaging_status_name, 
md_ins_date, md_upd_date, md_is_activ, md_is_delete, 
md_dwh_is_activ, md_dwh_date_from, md_dwh_date_to, md_dwh_status
)
select 		ods.messaging_status_id 
			, ods.messaging_status_name 
			, ods.md_ins_date 
			, ods.md_upd_date 
			, ods.md_is_activ 
			, ods.md_is_delete 
			, 1 as md_dwh_is_activ
			, (ods.md_ins_date - '1 day'::interval)::date as md_dwh_date_from 
			, '9999-12-31'::date as md_dwh_date_to 
			, 'I' as md_dwh_status
from 		blade10.blade_ods.messaging_status ods
full join	blade10.blade_dds.messaging_status dds
on			ods.messaging_status_id = dds.messaging_status_id
where 		1=1 
and 		dds.dwh_id is null;

-- (2) update old rows
update 	blade_dds.messaging_status
set md_dwh_date_to = sbq.md_dwh_date_to,
	md_dwh_is_activ = sbq.md_dwh_is_activ
from 
(
select 		dds.dwh_id 
			, ods.md_ins_date 
			, (ods.md_ins_date - '1 day'::interval)::date as md_dwh_date_to 
			, 0 as md_dwh_is_activ
from 		blade_ods.messaging_status ods
full join	blade_dds.messaging_status dds
on			ods.messaging_status_id = dds.messaging_status_id
where 		1=1
and 		dds.md_dwh_is_activ = 1
and			dds.md_dwh_status != 'D' 	
and 		ods.messaging_status_id is not null
and 		dds.dwh_id is not null
and  		md5(
			'' ||
			coalesce(ods.messaging_status_id::text, '') ||
			coalesce(ods.messaging_status_name::text, '')
			) != md5(
					'' ||
					coalesce(dds.messaging_status_id::text, '') ||
					coalesce(dds.messaging_status_name::text, '')
					)
) as sbq
WHERE blade_dds.messaging_status.dwh_id=sbq.dwh_id;

-- (3) new old rows 
insert into blade_dds.messaging_status (
    messaging_status_id, messaging_status_name,
    md_ins_date, md_upd_date, md_is_activ, md_is_delete,
    md_dwh_is_activ, md_dwh_date_from, md_dwh_date_to, md_dwh_status
)
select 		ods.messaging_status_id 
			, ods.messaging_status_name 
			, ods.md_ins_date 
			, ods.md_upd_date 
			, ods.md_is_activ 
			, ods.md_is_delete 
			, 1 as md_dwh_is_activ 
			, ods.md_ins_date as md_dwh_date_from 
			, '9999-12-31'::date as md_dwh_date_to 
			, 'U' as md_dwh_status 
from 		blade_ods.messaging_status ods
full join	blade_dds.messaging_status dds
on			ods.messaging_status_id = dds.messaging_status_id
where 		1=1
and 		dds.md_dwh_is_activ = 0;