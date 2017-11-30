drop table if exists jpc_wifi_null_train;
create table if not exists jpc_wifi_null_train as
select user_id,mall_id,shop_id,wifi_id,longitude,latitude from jpc_train where signal = 'null';

drop table if exists jpc_wifi_null_rule;
create table if not exists jpc_wifi_null_rule as
select shop_id,wifi_id,wifi_num from (
select shop_id,wifi_id,wifi_num, rank() over(partition by shop_id order by wifi_num desc) wifi_rank from(
select shop_id,wifi_id,count(wifi_id) wifi_num from jpc_wifi_null_train group by wifi_id,shop_id) temp) temp1 where wifi_rank = 1;

drop table if exists jpc_wifi_null_test;
create table if not exists jpc_wifi_null_test as
select user_id,row_id,mall_id,wifi_id,longitude,latitude from jpc_test where signal = 'null';

select count(distinct row_id) from jpc_wifi_null_test;   --882158
