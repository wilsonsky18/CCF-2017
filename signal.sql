--一个mall测试版
drop table if exists m_1006_wifi;
create table if not exists m_1006_wifi as
select wifi_id, shop_id, count(signal) num from(
select user_id, shop_id, wifi_id,signal from(select user_id, shop_id, wifi_id,signal, rank () over (distribute by user_id,shop_id sort by signal asc) a  from train_m_1006) b 
where b.a=1) c group by wifi_id,shop_id;

--用规则生成一个mall的预测集合
drop table if exists m_1006_temp;
create table if not exists m_1006_temp as
select row_id, shop_id,num,row_number() over (partition by row_id order by num desc) rank_wifi from(
select temp2.row_id,temp1.wifi_id,temp1.shop_id,temp1.num from m_1006_wifi temp1 join test_m_1006 temp2 on temp1.wifi_id=temp2.wifi_id) temp3;
rop table if exists m_1006_wifi_rule;
create table if not exists m_1006_wifi_rule as
select row_id,shop_id from m_1006_temp where rank_wifi=1;

--所有mall完整版
drop table if exists jpc_wifi_train ;
create table if not exists jpc_wifi_train as
select wifi_id, shop_id, count(signal) num from(
select user_id, shop_id, wifi_id,signal from(select user_id, shop_id, wifi_id,signal, rank () over (distribute by user_id,shop_id sort by signal asc) a  from jpc_second_train) b 
where b.a=1) c group by wifi_id,shop_id;

select count(*) from jpc_wifi_train;
select count(distinct wifi_id) from jpc_wifi_train;

--用规则生成预测集合
drop table if exists jpc_wifi_test_temp;
create table if not exists jpc_wifi_test_temp as
select row_id, shop_id,wifi_id,num,row_number() over (partition by row_id order by num desc) rank_wifi from(
select temp2.row_id,temp1.wifi_id,temp1.shop_id,temp1.num from jpc_wifi_train temp1 join jpc_second_test temp2 on temp1.wifi_id=temp2.wifi_id) temp3;
drop table if exists jpc_wifi_test ;
create table if not exists jpc_wifi_test as
select row_id,shop_id from jpc_wifi_test_temp where rank_wifi=1;

drop table if exists ant_tianchi_ccf_sl_predict;
create table if not exists ant_tianchi_ccf_sl_predict  as
select row_id,shop_id from
(select temp2.row_id,temp1.shop_id from jpc_wifi_test temp1  right outer join test_row_id temp2
on temp1.row_id=temp2.row_id) temp;

--检测缺失值数量 : 74843
select count(row_id) from (select row_id from ant_tianchi_ccf_sl_predict where ant_tianchi_ccf_sl_predict.shop_id is not null) temp;
select count(*) from ant_tianchi_ccf_sl_test

--不过滤wifi规则
drop table if exists jpc_wifi_train ;
create table if not exists jpc_wifi_train as
select wifi_id, shop_id, count(signal) num from(
select user_id, shop_id, wifi_id,signal from(select user_id, shop_id, wifi_id,signal, rank () over (distribute by user_id,shop_id sort by signal asc) a  from jpc_train) b 
where b.a=1) c group by wifi_id,shop_id;

drop table if exists jpc_wifi_test_temp;
create table if not exists jpc_wifi_test_temp as
select row_id, shop_id,wifi_id,num,row_number() over (partition by row_id order by num desc) rank_wifi from(
select temp2.row_id,temp1.wifi_id,temp1.shop_id,temp1.num from jpc_wifi_train temp1 join jpc_test temp2 on temp1.wifi_id=temp2.wifi_id) temp3;
drop table if exists jpc_wifi_test ;
create table if not exists jpc_wifi_test as
select row_id,shop_id from jpc_wifi_test_temp where rank_wifi=1;

drop table if exists ant_tianchi_ccf_sl_predict;
create table if not exists ant_tianchi_ccf_sl_predict  as
select row_id,shop_id from
(select temp2.row_id,temp1.shop_id from jpc_wifi_test temp1  right outer join test_row_id temp2
on temp1.row_id=temp2.row_id) temp;

--检测缺失值数量 : 58293
select count(row_id) from (select row_id from ant_tianchi_ccf_sl_predict where ant_tianchi_ccf_sl_predict.shop_id is not null) temp;
select count(*) from ant_tianchi_ccf_sl_test
