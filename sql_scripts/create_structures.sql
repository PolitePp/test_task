create schema schema_default;

create table schema_default.tb_users (
    uid uuid primary key,
    registration_date date,
    country varchar(200)
);

create table schema_default.tb_logins (
    user_uid uuid constraint fk_tb_logins_user_uid references schema_default.tb_users (uid),
    login varchar(20) primary key,
    account_type varchar(20)
);

create schema schema_billing;

create table schema_billing.tb_operations (
    operation_type varchar(20),
    operation_date date,
    login varchar(20) constraint fk_tb_operations_login references schema_default.tb_logins (login),
    amount money
);

create schema schema_orderstat;

create table schema_orderstat.tb_orders(
    login varchar(20) constraint fk_tb_orders_login references schema_default.tb_logins (login),
    order_close_date date
);