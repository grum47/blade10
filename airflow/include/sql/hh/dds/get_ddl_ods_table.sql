SELECT 'CREATE TABLE ' || 
        tablename ||
        ' (dwh_id serial PRIMARY KEY, ' || 
        string_agg("column" || ' ' || type, ', ') || 
        ', md_dwh_is_activ int4 not null, md_dwh_date_from date not null, md_dwh_date_to date not null, md_dwh_status char(1) not null);'
FROM (
    SELECT 
        c.table_name AS tablename,
        c.column_name AS "column",
        CASE 
            WHEN c.data_type = 'character varying' THEN 'VARCHAR(' || c.character_maximum_length || ')'
            WHEN c.data_type = 'character' THEN 'CHAR(' || c.character_maximum_length || ')'
            when c.data_type = 'ARRAY' then '_text'
            ELSE c.data_type
        END AS type
    FROM information_schema.columns c
    WHERE c.table_schema = '{{ params.pg_clean_schema }}' and c.table_name = '{{ params.table }}'
    ORDER BY c.ordinal_position
) AS table_definition
GROUP BY tablename;