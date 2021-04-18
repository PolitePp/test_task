create schema schema_default;

create table schema_default.tb_users (
    uid uuid primary key,
    registration_date date,
    country varchar(200) --Думал насчёт того, чтобы хранить id вместо текстового значения, но пришлось бы добавлять ещё один джойн.
);

create table schema_default.tb_logins (
    user_uid uuid constraint fk_tb_logins_user_uid references schema_default.tb_users (uid) not null,
    login varchar(20) primary key, --За основу взял банковские счета, которые состоят из 20 цифр. Т.к., как я понимаю, нет ограничения на то, что в начале стоит 0, то выбрал текстовое представление
    account_type varchar(20) --Думал хранить булево значение, что-то типа "is_real", но т.к. могут добавляться новые типы аккаунтов, решил, что это не дальновидно
);

create schema schema_billing;

create table schema_billing.tb_operations (
    operation_type varchar(20), --Здесь опять же можно было бы хранить булево значение, но если появится новая операция, то у нас проблемы
    operation_date date,
    login varchar(20) constraint fk_tb_operations_login references schema_default.tb_logins (login) not null,
    amount money
);

create schema schema_orderstat;

create table schema_orderstat.tb_orders(
    login varchar(20) constraint fk_tb_orders_login references schema_default.tb_logins (login) not null,
    order_close_date date
);