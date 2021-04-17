CREATE SCHEMA SERVICE;

create table service.countries (
    id int,
    country_name varchar
);

--https://www.html-code-generator.com/mysql/country-name-table
INSERT INTO service.countries
select 1,'Afghanistan' union all select
2,'Aland Islands' union all select
3,'Albania' union all select
4,'Algeria' union all select
5,'American Samoa' union all select
6,'Andorra' union all select
7,'Angola' union all select
8,'Anguilla' union all select
9,'Antarctica' union all select
10,'Antigua and Barbuda' union all select
11,'Argentina' union all select
12,'Armenia' union all select
13,'Aruba' union all select
14,'Australia' union all select
15,'Austria' union all select
16,'Azerbaijan' union all select
17,'Bahamas' union all select
18,'Bahrain' union all select
19,'Bangladesh' union all select
20,'Barbados' union all select
21,'Belarus' union all select
22,'Belgium' union all select
23,'Belize' union all select
24,'Benin' union all select
25,'Bermuda' union all select
26,'Bhutan' union all select
27,'Bolivia' union all select
28,'Bonaire, Sint Eustatius and Saba' union all select
29,'Bosnia and Herzegovina' union all select
30,'Botswana' union all select
31,'Bouvet Island' union all select
32,'Brazil' union all select
33,'British Indian Ocean Territory' union all select
34,'Brunei Darussalam' union all select
35,'Bulgaria' union all select
36,'Burkina Faso' union all select
37,'Burundi' union all select
38,'Cambodia' union all select
39,'Cameroon' union all select
40,'Canada' union all select
41,'Cape Verde' union all select
42,'Cayman Islands' union all select
43,'Central African Republic' union all select
44,'Chad' union all select
45,'Chile' union all select
46,'China' union all select
47,'Christmas Island' union all select
48,'Cocos Keeling Islands' union all select
49,'Colombia' union all select
50,'Comoros' union all select
51,'Congo' union all select
52,'Congo, Democratic Republic of the Congo' union all select
53,'Cook Islands' union all select
54,'Costa Rica' union all select
55,'Cote D''Ivoire' union all select
56,'Croatia' union all select
57,'Cuba' union all select
58,'Curacao' union all select
59,'Cyprus' union all select
60,'Czech Republic' union all select
61,'Denmark' union all select
62,'Djibouti' union all select
63,'Dominica' union all select
64,'Dominican Republic' union all select
65,'Ecuador' union all select
66,'Egypt' union all select
67,'El Salvador' union all select
68,'Equatorial Guinea' union all select
69,'Eritrea' union all select
70,'Estonia' union all select
71,'Ethiopia' union all select
72,'Falkland Islands Malvinas' union all select
73,'Faroe Islands' union all select
74,'Fiji' union all select
75,'Finland' union all select
76,'France' union all select
77,'French Guiana' union all select
78,'French Polynesia' union all select
79,'French Southern Territories' union all select
80,'Gabon' union all select
81,'Gambia' union all select
82,'Georgia' union all select
83,'Germany' union all select
84,'Ghana' union all select
85,'Gibraltar' union all select
86,'Greece' union all select
87,'Greenland' union all select
88,'Grenada' union all select
89,'Guadeloupe' union all select
90,'Guam' union all select
91,'Guatemala' union all select
92,'Guernsey' union all select
93,'Guinea' union all select
94,'Guinea-Bissau' union all select
95,'Guyana' union all select
96,'Haiti' union all select
97,'Heard Island and Mcdonald Islands' union all select
98,'Holy See Vatican City State' union all select
99,'Honduras' union all select
100,'Hong Kong' union all select
101,'Hungary' union all select
102,'Iceland' union all select
103,'India' union all select
104,'Indonesia' union all select
105,'Iran, Islamic Republic of' union all select
106,'Iraq' union all select
107,'Ireland' union all select
108,'Isle of Man' union all select
109,'Israel' union all select
110,'Italy' union all select
111,'Jamaica' union all select
112,'Japan' union all select
113,'Jersey' union all select
114,'Jordan' union all select
115,'Kazakhstan' union all select
116,'Kenya' union all select
117,'Kiribati' union all select
118,'Korea, Democratic People''s Republic of' union all select
119,'Korea, Republic of' union all select
120,'Kosovo' union all select
121,'Kuwait' union all select
122,'Kyrgyzstan' union all select
123,'Lao People''s Democratic Republic' union all select
124,'Latvia' union all select
125,'Lebanon' union all select
126,'Lesotho' union all select
127,'Liberia' union all select
128,'Libyan Arab Jamahiriya' union all select
129,'Liechtenstein' union all select
130,'Lithuania' union all select
131,'Luxembourg' union all select
132,'Macao' union all select
133,'Macedonia, the Former Yugoslav Republic of' union all select
134,'Madagascar' union all select
135,'Malawi' union all select
136,'Malaysia' union all select
137,'Maldives' union all select
138,'Mali' union all select
139,'Malta' union all select
140,'Marshall Islands' union all select
141,'Martinique' union all select
142,'Mauritania' union all select
143,'Mauritius' union all select
144,'Mayotte' union all select
145,'Mexico' union all select
146,'Micronesia, Federated States of' union all select
147,'Moldova, Republic of' union all select
148,'Monaco' union all select
149,'Mongolia' union all select
150,'Montenegro' union all select
151,'Montserrat' union all select
152,'Morocco' union all select
153,'Mozambique' union all select
154,'Myanmar' union all select
155,'Namibia' union all select
156,'Nauru' union all select
157,'Nepal' union all select
158,'Netherlands' union all select
159,'Netherlands Antilles' union all select
160,'New Caledonia' union all select
161,'New Zealand' union all select
162,'Nicaragua' union all select
163,'Niger' union all select
164,'Nigeria' union all select
165,'Niue' union all select
166,'Norfolk Island' union all select
167,'Northern Mariana Islands' union all select
168,'Norway' union all select
169,'Oman' union all select
170,'Pakistan' union all select
171,'Palau' union all select
172,'Palestinian Territory, Occupied' union all select
173,'Panama' union all select
174,'Papua New Guinea' union all select
175,'Paraguay' union all select
176,'Peru' union all select
177,'Philippines' union all select
178,'Pitcairn' union all select
179,'Poland' union all select
180,'Portugal' union all select
181,'Puerto Rico' union all select
182,'Qatar' union all select
183,'Reunion' union all select
184,'Romania' union all select
185,'Russian Federation' union all select
186,'Rwanda' union all select
187,'Saint Barthelemy' union all select
188,'Saint Helena' union all select
189,'Saint Kitts and Nevis' union all select
190,'Saint Lucia' union all select
191,'Saint Martin' union all select
192,'Saint Pierre and Miquelon' union all select
193,'Saint Vincent and the Grenadines' union all select
194,'Samoa' union all select
195,'San Marino' union all select
196,'Sao Tome and Principe' union all select
197,'Saudi Arabia' union all select
198,'Senegal' union all select
199,'Serbia' union all select
200,'Serbia and Montenegro' union all select
201,'Seychelles' union all select
202,'Sierra Leone' union all select
203,'Singapore' union all select
204,'Sint Maarten' union all select
205,'Slovakia' union all select
206,'Slovenia' union all select
207,'Solomon Islands' union all select
208,'Somalia' union all select
209,'South Africa' union all select
210,'South Georgia and the South Sandwich Islands' union all select
211,'South Sudan' union all select
212,'Spain' union all select
213,'Sri Lanka' union all select
214,'Sudan' union all select
215,'Suriname' union all select
216,'Svalbard and Jan Mayen' union all select
217,'Swaziland' union all select
218,'Sweden' union all select
219,'Switzerland' union all select
220,'Syrian Arab Republic' union all select
221,'Taiwan, Province of China' union all select
222,'Tajikistan' union all select
223,'Tanzania, United Republic of' union all select
224,'Thailand' union all select
225,'Timor-Leste' union all select
226,'Togo' union all select
227,'Tokelau' union all select
228,'Tonga' union all select
229,'Trinidad and Tobago' union all select
230,'Tunisia' union all select
231,'Turkey' union all select
232,'Turkmenistan' union all select
233,'Turks and Caicos Islands' union all select
234,'Tuvalu' union all select
235,'Uganda' union all select
236,'Ukraine' union all select
237,'United Arab Emirates' union all select
238,'United Kingdom' union all select
239,'United States' union all select
240,'United States Minor Outlying Islands' union all select
241,'Uruguay' union all select
242,'Uzbekistan' union all select
243,'Vanuatu' union all select
244,'Venezuela' union all select
245,'Viet Nam' union all select
246,'Virgin Islands, British' union all select
247,'Virgin Islands, U.s.' union all select
248,'Wallis and Futuna' union all select
249,'Western Sahara' union all select
250,'Yemen' union all select
251,'Zambia' union all select
252,'Zimbabwe';

CREATE OR REPLACE FUNCTION service.random_date (d1 DATE, d2 DATE) RETURN DATE
AS
BEGIN
    RETURN TO_TIMESTAMP(EXTRACT(EPOCH FROM d1) + RANDOMINT(FLOOR(EXTRACT(EPOCH FROM d2) - EXTRACT(EPOCH FROM d1))::INT));
END;

CREATE OR REPLACE FUNCTION service.random_datetime (d1 DATE, d2 DATE) RETURN DATETIME
AS
BEGIN
    RETURN TO_TIMESTAMP(EXTRACT(EPOCH FROM d1) + RANDOMINT(FLOOR(EXTRACT(EPOCH FROM d2) - EXTRACT(EPOCH FROM d1))::INT));
END;

CREATE OR REPLACE FUNCTION service.random_login (d1 DATETIME, d2 DATETIME) RETURN varchar(20)
AS
BEGIN
    RETURN cast(extract(EPOCH from d1) as int) || cast(extract(EPOCH from d2) as int);
END;

insert into schema_default.tb_users(uid, registration_date, country)
WITH seq AS (
    SELECT randomint(252) + 1 as num FROM (
        SELECT 1 FROM (
            SELECT date(0) + INTERVAL '1 second' AS se UNION ALL
            SELECT date(0) + INTERVAL '1000000 seconds' AS se ) a --generate 1 million row
        TIMESERIES tm AS '1 second' OVER(ORDER BY se)
    ) b
)
SELECT UUID_GENERATE() as uid
        , service.random_date('2010-01-01', current_date) as registration_date
        , c.country_name
FROM seq s
inner join service.countries c
on s.num = c.id;

insert into schema_default.tb_logins(user_uid, login, account_type)
select s1.uid
        , randomint(10) || substr(service.random_login(service.random_datetime(date'2002-01-01', current_date)
                                    , service.random_datetime(date'2002-01-01', current_date)), 2)
        , case when random() < 0.2 then 'demo' else 'real' end
from
(
    select uid
    from schema_default.tb_users
    order by uid
    limit 30000
) s1
cross join
(
    select 1
    union all
    select 1
    union all
    select 1
    union all
    select 1
    union all
    select 1
) s2;

insert into schema_default.tb_logins(user_uid, login, account_type)
select s1.uid
        , randomint(10) || substr(service.random_login(service.random_datetime(date'2002-01-01', current_date)
                                    , service.random_datetime(date'2002-01-01', current_date)), 2)
        , case when random() < 0.2 then 'demo' else 'real' end
from
(
    select uid
    from schema_default.tb_users
    order by uid
    limit 60000 offset 30000
) s1
cross join
(
    select 1
    union all
    select 1
    union all
    select 1
    union all
    select 1
) s2;

insert into schema_default.tb_logins(user_uid, login, account_type)
select s1.uid
        , randomint(10) || substr(service.random_login(service.random_datetime(date'2002-01-01', current_date)
                                    , service.random_datetime(date'2002-01-01', current_date)), 2)
        , case when random() < 0.2 then 'demo' else 'real' end
from
(
    select uid
    from schema_default.tb_users
    order by uid
    limit 90000 offset 90000
) s1
cross join
(
    select 1
    union all
    select 1
    union all
    select 1
) s2;

insert into schema_default.tb_logins(user_uid, login, account_type)
select s1.uid
        , randomint(10) || substr(service.random_login(service.random_datetime(date'2002-01-01', current_date)
                                    , service.random_datetime(date'2002-01-01', current_date)), 2)
        , case when random() < 0.2 then 'demo' else 'real' end
from
(
    select uid
    from schema_default.tb_users
    order by uid
    limit 120000 offset 180000
) s1
cross join
(
    select 1
    union all
    select 1
) s2;

insert into schema_default.tb_logins(user_uid, login, account_type)
select s1.uid
        , randomint(10) || substr(service.random_login(service.random_datetime(date'2002-01-01', current_date)
                                    , service.random_datetime(date'2002-01-01', current_date)), 2)
        , case when random() < 0.2 then 'demo' else 'real' end
from
(
    select uid
    from schema_default.tb_users
    order by uid
    limit 680000 offset 300000
) s1
cross join
(
    select 1
) s2;

insert into schema_billing.tb_operations(operation_type, operation_date, login, amount)
select 'deposit'
        , service.random_date(registration_date, current_date)
        , s1.login
        , case when randomint(10) = 9 then cast(random() * 100000 as money(17, 2))
                when randomint(10) between 6 and 8 then cast(random() * 10000 as money(17, 2))
                when randomint(10) between 4 and 5 then cast(random() * 1000 as money(17, 2))
                when randomint(10) between 2 and 3 then cast(random() * 100 as money(17, 2))
                else cast(random() * 100 as money(17, 2))
        end
from
(
    select login, tu.registration_date, randomint(10) as join_field
    from schema_default.tb_logins tl
    inner join schema_default.tb_users tu
    on tl.user_uid = tu.uid
) s1
inner join
(
    SELECT randomint(10) as join_field FROM (
        SELECT 1 FROM (
            SELECT date(0) + INTERVAL '1 second' AS se UNION ALL
            SELECT date(0) + INTERVAL '50 seconds' AS se ) a
        TIMESERIES tm AS '1 second' OVER(ORDER BY se)
    ) s2
) s2
on s1.join_field = s2.join_field;

insert into schema_billing.tb_operations(operation_type, operation_date, login, amount)
select 'withdrawal'
        , service.random_date(operation_date, current_date)
        , login
        , amount
from schema_billing.tb_operations
order by login
limit 100000;

insert into schema_billing.tb_operations(operation_type, operation_date, login, amount)
select 'withdrawal'
        , service.random_date(operation_date, current_date)
        , login
        , amount - randomint(cast(ceil(amount) as int) + 1)
from
(
    select operation_date
            , login
            , amount
            , randomint(10) as join_field
    from schema_billing.tb_operations
    order by login
    offset 100000
) s1
inner join
(
    SELECT row_number() over () + 2 as join_field FROM (
        SELECT 1 FROM (
            SELECT date(0) + INTERVAL '1 second' AS se UNION ALL
            SELECT date(0) + INTERVAL '10 seconds' AS se ) a
        TIMESERIES tm AS '1 second' OVER(ORDER BY se)
    ) s2
) s2
on s1.join_field = s2.join_field;

insert into schema_orderstat.tb_orders(login, order_close_date)
select s1.login
        , service.random_date(first_operation_date, current_date)
from
(
    select tl.login, min(top.operation_date) as first_operation_date, randomint(10) as join_field
    from schema_default.tb_logins tl
    inner join schema_billing.tb_operations top
    on tl.login = top.login
    group by tl.login
) s1
inner join
(
    SELECT randomint(10) as join_field FROM (
        SELECT 1 FROM (
            SELECT date(0) + INTERVAL '1 second' AS se UNION ALL
            SELECT date(0) + INTERVAL '50 seconds' AS se ) a
        TIMESERIES tm AS '1 second' OVER(ORDER BY se)
    ) s2
) s2
on s1.join_field = s2.join_field;

--create design for better performance
SELECT DESIGNER_CREATE_DESIGN('my_design');
SELECT DESIGNER_ADD_DESIGN_TABLES('my_design', 'schema_default.tb_users');
SELECT DESIGNER_ADD_DESIGN_TABLES('my_design', 'schema_default.tb_logins');
SELECT DESIGNER_ADD_DESIGN_TABLES('my_design', 'schema_billing.tb_operations');
SELECT DESIGNER_ADD_DESIGN_TABLES('my_design', 'schema_orderstat.tb_orders');
SELECT DESIGNER_ADD_DESIGN_QUERIES
     ('my_design',
     '/tmp/input/queries.txt',
     'true'
     );
SELECT DESIGNER_SET_DESIGN_TYPE('my_design', 'comprehensive');
SELECT DESIGNER_SET_OPTIMIZATION_OBJECTIVE('my_design', 'query');
SELECT DESIGNER_RUN_POPULATE_DESIGN_AND_DEPLOY
   ('my_design',
    '/tmp/output/my_design_projections.sql',
    '/tmp/output/my_design_deploy.sql',
    'True', --Analyzes statistics
    'True', --Doesn't deploy the design
    'False', --Doesn't drop the design after deployment
    'False' --Stops if it encounters an error
    );
SELECT DESIGNER_DROP_DESIGN('my_design');