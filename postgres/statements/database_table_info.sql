select pg_total_relation_size('_timescaledb_internal._hyper_29662_893648_chunk')



select * from  information_schema.tables

select * from _timescaledb_catalog.hypertable

select * from pg_class where relname like '%_airbyte_raw_dcim_devices%'

select * from pg_inherits where inhrelid = 66461

select * from pg_class where oid = 71064

select * from pg_namespace where oid = 43329

select * from pg_type where oid =0

select pg_total_relation_size('airbyte_nautobot._airbyte_raw_dcim_devices_pkey')

with
classes as (
    select oid, relname, relnamespace from pg_class
),
inheritance as (
    select inhrelid, inhparent as parent_oid from pg_inherits
),
schemas as (
    select oid as schema_oid, nspname as schema_name from pg_namespace
),
joined_namespaces as (
    select * from classes left join schemas on classes.relnamespace = schema_oid
),
joined_inheritance as (
    select
        oid as child_oid,
        relname as child_relname,
        parent_oid
    from joined_namespaces
    left join inheritance on oid=inhrelid
),
joined_parents as (
    select
        joined_namespaces.oid as parent_oid,
        child_oid,
        schema_name,
        joined_namespaces.relname as parent_relname,
        child_relname
    from joined_namespaces
    left join joined_inheritance on parent_oid = joined_namespaces.oid
)

select * from joined_parents where parent_relname = 'gpu_fields'
select pg_total_relation_size('dbt_intermediate.int_building_co2e')


with all_tables as (
    with
    classes as (
        select
            oid,
            relname,
            relkind,
            relnamespace
        from pg_class
        where relkind in ('r', 'm', 'p')
    ),
    inheritance as (
        select inhrelid, inhparent as parent_oid from pg_inherits
    ),
    schemas as (
        select oid as schema_oid, nspname as schema_name from pg_namespace
    ),
    joined_namespaces as (
        select * from classes left join schemas on classes.relnamespace = schema_oid
    ),
    joined_inheritance as (
        select
            oid as child_oid,
            relname as child_relname,
            parent_oid,
            schema_name as child_schema_name
        from joined_namespaces
        left join inheritance on oid=inhrelid
    ),
    joined_parents as (
        select
            joined_namespaces.oid as parent_oid,
            child_oid,
            schema_name,
            child_schema_name,
            joined_namespaces.relname as parent_relname,
            child_relname
        from joined_inheritance
        left join joined_namespaces on parent_oid = joined_namespaces.oid
    )
    select
        parent_oid,
        child_oid,
        schema_name,
        child_schema_name,
        parent_relname,
        child_relname
    from joined_parents
)


select sum(pg_total_relation_size('"' || child_schema_name || '"."' || child_relname || '"') ) - pg_database_size('bp') from all_tables
select current_database() as database, parent_oid, child_oid, schema_name, child_schema_name, parent_relname, child_relname, pg_total_relation_size('"' || child_schema_name || '"."' || child_relname || '"') from all_tables
where child_relname like '%raw_dcim_devices%'

select pg_total_relation_size('airbyte_nautobot.bak._airbyte_raw_dcim_devices')


select sum(pg_total_relation_size(child_schema_name || '.' || child_relname)) from all_tables
select pg_total_relation_size('airbyte_nautobot.bak._airbyte_raw_dcim_devices')

select t1.datname AS db_name,
       pg_database_size(t1.datname) as db_size
from pg_database t1
order by pg_database_size(t1.datname) desc;

SELECT *
FROM information_schema.tables
WHERE table_schema = 'airbyte_nautobot' AND table_name = '_airbyte_raw_dcim_devices';

SELECT pg_total_relation_size('"airbyte_nautobot.bak"."_airbyte_raw_dcim_devices"');



