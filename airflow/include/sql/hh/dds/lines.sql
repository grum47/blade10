CREATE TABLE IF NOT EXISTS blade_dds.lines (
    dwh_id serial PRIMARY KEY, 
    line_id bigint, 
    line_name text, 
    line_hex_color text, 
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
insert into blade_dds.lines (
line_id, line_name, line_hex_color,
md_ins_date, md_upd_date, md_is_activ, md_is_delete, 
md_dwh_is_activ, md_dwh_date_from, md_dwh_date_to, md_dwh_status
)
select 		ods.line_id 
			, ods.line_name 
            , ods.line_hex_color
			, ods.md_ins_date 
			, ods.md_upd_date 
			, ods.md_is_activ 
			, ods.md_is_delete 
			, 1 as md_dwh_is_activ
			, (ods.md_ins_date - '1 day'::interval)::date as md_dwh_date_from 
			, '9999-12-31'::date as md_dwh_date_to 
			, 'I' as md_dwh_status
from 		blade10.blade_ods.lines ods
full join	blade10.blade_dds.lines dds
on			ods.line_id = dds.line_id
where 		1=1 
and 		dds.dwh_id is null;

-- (2) update old rows
update 	blade_dds.lines
set md_dwh_date_to = sbq.md_dwh_date_to,
	md_dwh_is_activ = sbq.md_dwh_is_activ
from 
(
select 		dds.dwh_id 
			, ods.md_ins_date 
			, (ods.md_ins_date - '1 day'::interval)::date as md_dwh_date_to 
			, 0 as md_dwh_is_activ
from 		blade_ods.lines ods
full join	blade_dds.lines dds
on			ods.line_id = dds.line_id
where 		1=1
and 		dds.md_dwh_is_activ = 1
and			dds.md_dwh_status != 'D' 	
and 		ods.line_id is not null
and 		dds.dwh_id is not null
and  		md5(
			'' ||
			coalesce(ods.line_id::text, '') ||
			coalesce(ods.line_name::text, '') ||
            coalesce(ods.line_hex_color::text, '')
			) != md5(
					'' ||
					coalesce(dds.line_id::text, '') ||
					coalesce(dds.line_name::text, '') ||
                    coalesce(dds.line_hex_color::text, '')
					)
) as sbq
WHERE blade_dds.lines.dwh_id=sbq.dwh_id;

-- (3) new old rows 
insert into blade_dds.lines (
    line_id, line_name, line_hex_color,
    md_ins_date, md_upd_date, md_is_activ, md_is_delete,
    md_dwh_is_activ, md_dwh_date_from, md_dwh_date_to, md_dwh_status
)
select 		ods.line_id 
			, ods.line_name 
            , ods.line_hex_color
			, ods.md_ins_date 
			, ods.md_upd_date 
			, ods.md_is_activ 
			, ods.md_is_delete 
			, 1 as md_dwh_is_activ 
			, ods.md_ins_date as md_dwh_date_from 
			, '9999-12-31'::date as md_dwh_date_to 
			, 'U' as md_dwh_status 
from 		blade_ods.lines ods
full join	blade_dds.lines dds
on			ods.line_id = dds.line_id
where 		1=1
and 		dds.md_dwh_is_activ = 0;