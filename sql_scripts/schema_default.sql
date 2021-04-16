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