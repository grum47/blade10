CREATE TABLE IF NOT EXISTS blade_dds.stations (
    dwh_id serial PRIMARY KEY, 
    station_id bigint, 
    station_name text, 
    station_order bigint, 
    lat double precision, 
    lon double precision, 
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
insert into blade_dds.stations (
station_id, station_name, station_order, lat, lon,
md_ins_date, md_upd_date, md_is_activ, md_is_delete, 
md_dwh_is_activ, md_dwh_date_from, md_dwh_date_to, md_dwh_status
)
select 		ods.station_id 
			, ods.station_name 
            , ods.station_order
            , ods.lat
            , ods.lon
			, ods.md_ins_date 
			, ods.md_upd_date 
			, ods.md_is_activ 
			, ods.md_is_delete 
			, 1 as md_dwh_is_activ
			, (ods.md_ins_date - '1 day'::interval)::date as md_dwh_date_from 
			, '9999-12-31'::date as md_dwh_date_to 
			, 'I' as md_dwh_status
from 		blade10.blade_ods.stations ods
full join	blade10.blade_dds.stations dds
on			ods.station_id = dds.station_id
where 		1=1 
and 		dds.dwh_id is null;

-- (2) update old rows
update 	blade_dds.stations
set md_dwh_date_to = sbq.md_dwh_date_to,
	md_dwh_is_activ = sbq.md_dwh_is_activ
from 
(
select 		dds.dwh_id 
			, ods.md_ins_date 
			, (ods.md_ins_date - '1 day'::interval)::date as md_dwh_date_to 
			, 0 as md_dwh_is_activ
from 		blade_ods.stations ods
full join	blade_dds.stations dds
on			ods.station_id = dds.station_id
where 		1=1
and 		dds.md_dwh_is_activ = 1
and			dds.md_dwh_status != 'D' 	
and 		ods.station_id is not null
and 		dds.dwh_id is not null
and  		md5(
			'' ||
			coalesce(ods.station_id::text, '') ||
			coalesce(ods.station_name::text, '') ||
            coalesce(ods.station_order::text, '') ||
            coalesce(ods.lat::text, '') ||
            coalesce(ods.lon::text, '')
			) != md5(
					'' ||
					coalesce(dds.station_id::text, '') ||
					coalesce(dds.station_name::text, '') ||
                    coalesce(dds.station_order::text, '') ||
                    coalesce(dds.lat::text, '') ||
                    coalesce(dds.lon::text, '')
					)
) as sbq
WHERE blade_dds.stations.dwh_id=sbq.dwh_id;

-- (3) new old rows 
insert into blade_dds.stations (
    station_id, station_name, station_order, lat, lon,
    md_ins_date, md_upd_date, md_is_activ, md_is_delete,
    md_dwh_is_activ, md_dwh_date_from, md_dwh_date_to, md_dwh_status
)
select 		ods.station_id 
			, ods.station_name 
            , ods.station_order
            , ods.lat
            , ods.lon
			, ods.md_ins_date 
			, ods.md_upd_date 
			, ods.md_is_activ 
			, ods.md_is_delete 
			, 1 as md_dwh_is_activ 
			, ods.md_ins_date as md_dwh_date_from 
			, '9999-12-31'::date as md_dwh_date_to 
			, 'U' as md_dwh_status 
from 		blade_ods.stations ods
full join	blade_dds.stations dds
on			ods.station_id = dds.station_id
where 		1=1
and 		dds.md_dwh_is_activ = 0;