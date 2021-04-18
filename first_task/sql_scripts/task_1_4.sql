--Фильтруем информацию только по депозитам, находим номера строк в разрезе клиента и дате операции
select *
from
(
    select tl.user_uid
            , tl.login
            , top.operation_date
            , row_number() over (partition by tl.user_uid order by top.operation_date) as rn
    from schema_default.tb_logins tl
    inner join schema_billing.tb_operations top
    on tl.login = top.login
    where top.operation_type = 'deposit'
) s1
where rn < 4;