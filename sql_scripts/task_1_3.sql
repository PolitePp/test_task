select s1.country
        , count(*) as count_users
        , count(case when avg_dep >= 1000 then 1 else null end) as count_users_big_dep
from
(
select tu.country
        , tu.uid
        , sum(top.amount) / count(distinct tl.login) as avg_dep
from schema_default.tb_users tu
inner join schema_default.tb_logins tl
on tu.uid = tl.user_uid
inner join schema_billing.tb_operations top
on tl.login = top.login
where top.operation_type = 'deposit'
group by tu.country
            , tu.uid
) s1
group by s1.country;