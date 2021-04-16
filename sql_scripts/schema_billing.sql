create schema schema_billing;

create table schema_billing.tb_operations(
        operation_type varchar(20),
        operation_date date,
        login varchar(20) constraint fk_tb_operations_login references schema_default.tb_logins (login),
        amount money
    );