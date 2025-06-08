drop table if exists stg.delivery_system_deliveries;

create table if not exists stg.delivery_system_deliveries (
    id serial not null,
    delivery_id varchar unique,
    delivery_ts timestamp,
    payload jsonb
);