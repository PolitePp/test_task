CREATE PROJECTION schema_default.tb_users_DBD_1_rep_my_design
(
 uid,
 registration_date ENCODING RLE,
 country
)
AS
 SELECT tb_users.uid,
        tb_users.registration_date,
        tb_users.country
 FROM schema_default.tb_users
 ORDER BY tb_users.registration_date,
          tb_users.country,
          tb_users.uid
UNSEGMENTED ALL NODES;



\echo NOTICE: The above create projection statement could error out if design created and implemented in the same cluster

CREATE PROJECTION schema_default.tb_logins_DBD_2_rep_my_design
(
 user_uid,
 login,
 account_type ENCODING RLE
)
AS
 SELECT tb_logins.user_uid,
        tb_logins.login,
        tb_logins.account_type
 FROM schema_default.tb_logins
 ORDER BY tb_logins.account_type,
          tb_logins.login
UNSEGMENTED ALL NODES;



\echo NOTICE: The above create projection statement could error out if design created and implemented in the same cluster

CREATE PROJECTION schema_default.tb_logins_DBD_3_rep_my_design
(
 user_uid,
 login
)
AS
 SELECT tb_logins.user_uid,
        tb_logins.login
 FROM schema_default.tb_logins
 ORDER BY tb_logins.login
UNSEGMENTED ALL NODES;



\echo NOTICE: The above create projection statement could error out if design created and implemented in the same cluster

CREATE PROJECTION schema_billing.tb_operations_DBD_4_rep_my_design
(
 operation_type ENCODING RLE,
 operation_date ENCODING DELTARANGE_COMP,
 login ENCODING RLE,
 amount ENCODING DELTARANGE_COMP
)
AS
 SELECT tb_operations.operation_type,
        tb_operations.operation_date,
        tb_operations.login,
        tb_operations.amount
 FROM schema_billing.tb_operations
 ORDER BY tb_operations.login,
          tb_operations.operation_type,
          tb_operations.operation_date,
          tb_operations.amount
UNSEGMENTED ALL NODES;



\echo NOTICE: The above create projection statement could error out if design created and implemented in the same cluster

CREATE PROJECTION schema_orderstat.tb_orders_DBD_5_rep_my_design
(
 login ENCODING RLE,
 order_close_date ENCODING DELTARANGE_COMP
)
AS
 SELECT tb_orders.login,
        tb_orders.order_close_date
 FROM schema_orderstat.tb_orders
 ORDER BY tb_orders.login,
          tb_orders.order_close_date
UNSEGMENTED ALL NODES;



\echo NOTICE: The above create projection statement could error out if design created and implemented in the same cluster



