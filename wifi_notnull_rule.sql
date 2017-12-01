--不过滤wifi规则
drop table if exists jpc_wifi_notnull_train ;
create table if not exists jpc_wifi_notnull_train as
select wifi_id, shop_id, count(signal) num from(
select user_id, shop_id, wifi_id,signal from(select user_id, shop_id, wifi_id,signal, rank () over (distribute by user_id,shop_id sort by signal asc) a  from jpc_train where signal != 'null') b 
where b.a=1) c group by wifi_id,shop_id;

drop table if exists  jpc_wifi_not_null_test; 
drop table if exists jpc_wifi_notnull_test;
create table if not exists jpc_wifi_notnull_test as
select * from jpc_test where signal != 'null';

drop table if exists jpc_wifi_notnull_test_temp;
create table if not exists jpc_wifi_notnull_test_temp as
select row_id, shop_id,wifi_id,num,row_number() over (partition by row_id order by num desc) rank_wifi from(
select temp2.row_id,temp1.wifi_id,temp1.shop_id,temp1.num from jpc_wifi_notnull_train temp1 join jpc_wifi_notnull_test temp2 on temp1.wifi_id=temp2.wifi_id) temp3;

drop table if exists jpc_wifi_notnull_test_result ;
create table if not exists jpc_wifi_notnull_test_result as
select row_id,shop_id from jpc_wifi_notnull_test_temp where rank_wifi=1;

select count(*) from  jpc_wifi_notnull_test where shop_id is not null;   -- 1505285
