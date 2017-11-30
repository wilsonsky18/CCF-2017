--不过滤wifi规则
drop table if exists jpc_wif_notnull_train ;
create table if not exists jpc_wifi_train as
select wifi_id, shop_id, count(signal) num from(
select user_id, shop_id, wifi_id,signal from(select user_id, shop_id, wifi_id,signal, rank () over (distribute by user_id,shop_id sort by signal asc) a  from jpc_train where signal != 'null') b 
where b.a=1) c group by wifi_id,shop_id;

drop table if exists jpc_wifi_notnull_test_temp;
create table if not exists jpc_wifi_notnull_test_temp as
select row_id, shop_id,wifi_id,num,row_number() over (partition by row_id order by num desc) rank_wifi from(
select temp2.row_id,temp1.wifi_id,temp1.shop_id,temp1.num from jpc_wifi_train temp1 join jpc_test temp2 where temp2.signal != 'null' on temp1.wifi_id=temp2.wifi_id) temp3;
drop table if exists jpc_wifi_test ;
create table if not exists jpc_wifi_test as
select row_id,shop_id from jpc_wifi_test_temp where rank_wifi=1;

drop table if exists ant_tianchi_ccf_sl_predict;
create table if not exists ant_tianchi_ccf_sl_predict  as
select row_id,shop_id from
(select temp2.row_id,temp1.shop_id from jpc_wifi_test temp1  right outer join test_row_id temp2
on temp1.row_id=temp2.row_id) temp;
