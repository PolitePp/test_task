create schema schema_orderstat;

create table schema_orderstat.tb_orders(
    login varchar(20) constraint fk_tb_orders_login references schema_default.tb_logins (login),
    order_close_date date
);