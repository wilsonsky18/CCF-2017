drop table if exists jpc_wifi_null_train;
create table if not exists jpc_wifi_null_train as
select user_id,mall_id,shop_id,wifi_id,longitude,latitude from jpc_train where signal = 'null';

drop table if exists jpc_wifi_null_rule;
create table if not exists jpc_wifi_null_rule as
select shop_id,wifi_id,wifi_num from (
select shop_id,wifi_id,wifi_num, rank() over(partition by shop_id order by wifi_num desc) wifi_rank from(
select shop_id,wifi_id,count(wifi_id) wifi_num from jpc_wifi_null_train group by wifi_id,shop_id) temp) temp1 where wifi_rank = 1;

drop table if exists jpc_wifi_null_rule_final;
create table if not exists jpc_wifi_null_rule_final as
select wifi_id,shop_id,wifi_num from (
select *,row_number() over (partition by wifi_id order by wifi_num desc) rank_wifi from jpc_wifi_null_rule) temp where rank_wifi=1; 
select count(*) from jpc_wifi_null_rule_final ; --25212 wifi_id

drop table if exists jpc_wifi_null_test;
create table if not exists jpc_wifi_null_test as
select user_id,row_id,mall_id,wifi_id,longitude,latitude from jpc_test where signal = 'null';

select count(distinct row_id) from jpc_wifi_null_test;   --882158

drop table if exists jpc_wifi_null_test_result;
create table if not exists jpc_wifi_null_test_result as
select row_id, shop_id from(
select temp2.row_id,temp1.wifi_id,temp1.shop_id from jpc_wifi_null_rule  temp1 join jpc_wifi_null_test  temp2 on temp1.wifi_id=temp2.wifi_id) temp3;

select count(*) from jpc_wifi_null_test_result where shop_id is not null; --939227
