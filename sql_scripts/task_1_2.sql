-- среднее время перехода пользователей между этапами воронки.
-- Т.к. не указано, в чём измерить среднее время, решил рассчитывать информацию в днях
with get_users as (
    --Получаем клиентов, которые были созданы в течении последних 90 дней
    select tu.country
            , tu.uid
            , tu.registration_date
    from schema_default.tb_users tu
    where tu.registration_date >= current_date - 90
),
get_days_between_registration_deposit as (
    --Отбираем информацию только о реальных счетах и операциях типа депозит.
    -- Находим минимальную дату операции (она не может быть меньше, чем дата регистрация клиента)
    -- , поэтому, для уменьшения выборки можно добавить условие на промежуток в 90 дней и сюда
    -- Группируем по стране, клиенту.
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
            and top.operation_date >= current_date - 90
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
    --На основе запроса get_days_between_registration_deposit, где у нас есть информация о первой дате депозита
    -- , находим информацию о первой сделке. Важно помнить о том, что её может не быть, но клиент должен быть в выборке
    -- Однако, тогда непонятно какую информацию заносить в кол-во дней. Решил, что должен быть null.
    -- Тогда у нас этот клиент останется в выборке, но при этом в расчёте среднего времени участвовать не будет
    -- Здесь так же добавляем условие на последние 90 дней для уменьшения выборки
    select gd.country
            , gd.uid
            , gd.days_between_registration_deposit
            , datediff('day', gd.min_operation_date, min(tor.order_close_date)) as days_between_deposit_order
    from get_days_between_registration_deposit gd
    inner join schema_default.tb_logins tl
    on gd.uid = tl.user_uid
    left join schema_orderstat.tb_orders tor
    on tl.login = tor.login
    where tl.account_type = 'real'
            and tor.order_close_date >= current_date - 90
    group by gd.country
                , gd.uid
                , gd.days_between_registration_deposit
                , gd.min_operation_date
) s1
group by s1.country
order by 2 desc;

