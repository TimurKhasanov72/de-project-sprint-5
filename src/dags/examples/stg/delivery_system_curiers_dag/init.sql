drop table if exists stg.delivery_system_couriers;

create table if not exists stg.delivery_system_couriers (
    courier_id varchar,
    courier_name varchar
);