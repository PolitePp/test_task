with get_users as (
    select tu.country
            , tu.uid
            , tu.registration_date
--             , coalesce(datediff('day', min(top.operation_date), min(tor.order_close_date)), 0) as days_between_deposit_order
    from schema_default.tb_users tu
    where tu.registration_date >= current_date - 90
),
get_days_between_registration_deposit as (
    select gu.country
            , gu.uid
            , min(top.operation_date) as min_operation_date
            , datediff('day', gu.registration_date, min(top.operation_date)) as days_between_registration_deposit
    from get_users gu
    inner join schema_default.tb_logins tl
    on gu.uid = tl.user_uid
    inner join schema_billing.tb_operations top
    on tl.login = top.login
    where tl.account_type = 'real'
            and top.operation_type = 'deposit'
    group by gu.country
                , gu.uid
                , gu.registration_date
)
select s1.country
        , count(*) as count_users
        , avg(days_between_registration_deposit) as avg_days_btw_reg_dep
        , avg(days_between_deposit_order) as avg_days_btw_dep_ord
from
(
select gd.country
        , gd.uid
        , gd.days_between_registration_deposit
        , coalesce(datediff('day', gd.min_operation_date, min(tor.order_close_date)), 0) as days_between_deposit_order
from get_days_between_registration_deposit gd
inner join schema_default.tb_logins tl
on gd.uid = tl.user_uid
left join schema_orderstat.tb_orders tor
on tl.login = tor.login
where tl.account_type = 'real'
group by gd.country
            , gd.uid
            , gd.days_between_registration_deposit
            , gd.min_operation_date
) s1
group by s1.country
order by 2 desc;

