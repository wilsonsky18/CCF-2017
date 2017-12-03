--不过滤wifi规则
drop table if exists jpc_wifi_notnull_test;
create table if not exists jpc_wifi_notnull_test as
select * from jpc_test where signal != 'null';

drop table if exists jpc_wifi_null_train;
create table if not exists jpc_wifi_null_train as
select * from jpc_train where signal = 'null';

drop table if exists jpc_wifi_null_test;
create table if not exists jpc_wifi_null_test as
select * from jpc_test where signal = 'null';

--notnull wifi计算，按强度为标准
drop table if exists jpc_wifi_notnull_rule ;
create table if not exists jpc_wifi_notnull_rule as
select wifi_id, shop_id, count(signal) num from(
select user_id, shop_id, wifi_id,signal from(select user_id, shop_id, wifi_id,signal, rank () over (distribute by user_id,shop_id sort by signal asc) a  from jpc_train where signal != 'null') b 
where b.a=1) c group by wifi_id,shop_id;

drop table if exists jpc_wifi_notnull_test_temp;
create table if not exists jpc_wifi_notnull_test_temp as
select row_id, shop_id,wifi_id,num,row_number() over (partition by row_id order by num desc) rank_wifi from(
select temp2.row_id,temp1.wifi_id,temp1.shop_id,temp1.num from jpc_wifi_notnull_rule temp1 join jpc_wifi_notnull_test temp2 on temp1.wifi_id=temp2.wifi_id) temp3;

drop table if exists jpc_wifi_notnull_test_result ;
create table if not exists jpc_wifi_notnull_test_result as
select row_id,shop_id from jpc_wifi_notnull_test_temp where rank_wifi=1;

select count(distinct row_id) from  jpc_wifi_notnull_test;  -- 1519961
select count(*) from  jpc_wifi_notnull_test_result where shop_id is not null;   -- 1505285

--null wifi 按一shop搜索到不同wifi次数排序，统计的是每个shop下不同的wifi个数。
--drop table if exists jpc_wifi_null_rule;
--create table if not exists jpc_wifi_null_rule as
--select shop_id,wifi_id,wifi_num from (
--select shop_id,wifi_id,wifi_num, rank() over(partition by shop_id order by wifi_num desc) wifi_rank from(
--select shop_id,wifi_id,count(wifi_id) wifi_num from jpc_wifi_null_train group by wifi_id,shop_id) temp) temp1 where wifi_rank = 1;

--select count(distinct wifi_id) from jpc_wifi_null_rule --25212 distinct wifi_id
--select count(*) from jpc_wifi_null_rule; --30137

--null wifi 按一个wifi被不同shop搜索到次数排序，统计的是每个wifi下不同shop的个数。
drop table if exists jpc_wifi_null_rule;
create table if not exists jpc_wifi_null_rule as
select wifi_id,shop_id,shop_num from (
select wifi_id,shop_id,shop_num, rank() over(partition by wifi_id order by shop_num desc) shop_rank from(
select shop_id,wifi_id,count(shop_id) shop_num from jpc_wifi_null_train group by wifi_id,shop_id)  temp) temp1 where shop_rank = 1;

select count(distinct wifi_id) from jpc_wifi_null_rule; --290715 distinct wifi_id
select count(*) from jpc_wifi_null_rule; --331801

drop table if exists jpc_wifi_null_test_result;
create table if not exists jpc_wifi_null_test_result as
select row_id,shop_id from (
select row_id, shop_id,rank() over(partition by row_id order by shop_num) shop_num from(
select temp2.row_id,temp1.wifi_id,temp1.shop_id,temp1.shop_num from jpc_wifi_null_rule temp1 join jpc_wifi_null_test  temp2 on temp1.wifi_id=temp2.wifi_id) temp3) temp4 
where shop_num = 1;
select count(distinct row_id) from jpc_wifi_null_test;   --882158
select count(*) from jpc_wifi_null_test_result where shop_id is not null; --851571

--union null和notnull的结果
drop table if exists union_null_notnull_result;
create table if not exists union_null_notnull_result as
select * from prj_tc_231620_98365_yrdets.jpc_wifi_notnull_test_result a
union 
select * from prj_tc_231620_98365_yrdets.jpc_wifi_null_test_result b; 

--生成预测结果,这里对union的结果又进行了row_number是因为，row_id有重复项。
drop table if exists ant_tianchi_ccf_sl_predict;
create table if not exists ant_tianchi_ccf_sl_predict  as
select row_id,shop_id from (
select row_id,shop_id,row_number() over(partition by row_id order by shop_id) row_num from
(select temp2.row_id,temp1.shop_id from union_null_notnull_result temp1 right outer join test_row_id temp2
on temp1.row_id=temp2.row_id) temp ) temp1 where row_num=1;

select count(*) from  union_null_notnull_result; --2356856
select count(distinct row_id) from ant_tianchi_ccf_sl_predict result where result.shop_id is null; --65839
select count(*) from  ant_tianchi_ccf_sl_test;  --2402119
select count(*) from  ant_tianchi_ccf_sl_predict; --2402119






--不过滤wifi规则 append_id 版本
drop table if exists jpc_wifi_notnull_test;
create table if not exists jpc_wifi_notnull_test as
select * from jpc_test where signal != 'null';

drop table if exists jpc_wifi_null_train;
create table if not exists jpc_wifi_null_train as
select * from jpc_train where signal = 'null';

drop table if exists jpc_wifi_null_test;
create table if not exists jpc_wifi_null_test as
select * from jpc_test where signal = 'null';

--notnull wifi计算，按强度为标准
drop table if exists jpc_wifi_notnull_rule ;
create table if not exists jpc_wifi_notnull_rule as
select wifi_id, shop_id, count(signal) num from(
select user_id, shop_id, wifi_id,signal from(select user_id, shop_id, wifi_id,signal, rank () over (distribute by user_id,shop_id sort by signal asc) a  from jpc_train where signal != 'null') b 
where b.a=1) c group by wifi_id,shop_id;

drop table if exists jpc_wifi_notnull_test_temp;
create table if not exists jpc_wifi_notnull_test_temp as
select row_id, shop_id,wifi_id,num,row_number() over (partition by row_id order by num desc) rank_wifi from(
select temp2.row_id,temp1.wifi_id,temp1.shop_id,temp1.num from jpc_wifi_notnull_rule temp1 join jpc_wifi_notnull_test temp2 on temp1.wifi_id=temp2.wifi_id) temp3;

drop table if exists jpc_wifi_notnull_test_result ;
create table if not exists jpc_wifi_notnull_test_result as
select row_id,shop_id from jpc_wifi_notnull_test_temp where rank_wifi=1;

select count(distinct row_id) from  jpc_wifi_notnull_test;  -- 1519961
select count(*) from  jpc_wifi_notnull_test_result where shop_id is not null;   -- 1505285

--null wifi 按搜索到次数排序
drop table if exists jpc_wifi_null_test_result;
create table if not exists jpc_wifi_null_test_result as
select row_id,shop_id from (
select row_id,shop_id, rank() over(partition by row_id order by wifi_num desc) rank_wifi from (
select temp2.row_id,temp1.wifi_id,temp1.shop_id,temp1.wifi_num from jpc_wifi_null_rule temp1 join jpc_wifi_null_test  temp2 on temp1.wifi_id=temp2.wifi_id) temp3)
temp4 where rank_wifi=1;
select count(distinct row_id) from jpc_wifi_null_test;   --882158
select count(*) from jpc_wifi_null_test_result where shop_id is not null; --715117

--union null和notnull的结果
drop table if exists union_null_notnull_result;
create table if not exists union_null_notnull_result as
select * from prj_tc_231620_98365_yrdets.jpc_wifi_notnull_test_result a
union 
select * from prj_tc_231620_98365_yrdets.jpc_wifi_null_test_result b; 

--生成预测结果
drop table if exists ant_tianchi_ccf_sl_predict;
create table if not exists ant_tianchi_ccf_sl_predict  as
select row_id,shop_id from (
select row_id,shop_id,row_number() over(partition by row_id order by shop_id) row_num from
(select temp2.row_id,temp1.shop_id from union_null_notnull_result temp1 right outer join test_row_id temp2
on temp1.row_id=temp2.row_id) temp ) temp1 where row_num=1;

select count(*) from  union_null_notnull_result; --1972392
select count(distinct row_id) from ant_tianchi_ccf_sl_predict result where result.shop_id is null; --429727
select count(*) from  ant_tianchi_ccf_sl_test;  --2402119
select count(*) from  ant_tianchi_ccf_sl_predict; --2402119

--不过滤wifi规则
drop table if exists jpc_wifi_notnull_test;
create table if not exists jpc_wifi_notnull_test as
select * from jpc_test where signal != 'null';

drop table if exists jpc_wifi_null_train;
create table if not exists jpc_wifi_null_train as
select * from jpc_train_index where signal = 'null';

drop table if exists jpc_wifi_null_test;
create table if not exists jpc_wifi_null_test as
select * from jpc_test_index where signal = 'null';

--notnull wifi计算，按强度为标准
drop table if exists jpc_wifi_notnull_rule ;
create table if not exists jpc_wifi_notnull_rule as
select wifi_id, shop_id, count(shop_id) num from(
select user_id, shop_id, wifi_id,signal from(select user_id, shop_id, wifi_id,signal, rank () over (distribute by append_id sort by signal asc) a  from jpc_train_index where signal != 'null') b 
where b.a=1) c group by wifi_id,shop_id;

drop table if exists jpc_wifi_notnull_test_temp;
create table if not exists jpc_wifi_notnull_test_temp as
select row_id, shop_id,wifi_id,num,rank() over (partition by row_id order by num desc) shop_rank from(
select temp2.row_id,temp1.wifi_id,temp1.shop_id,temp1.num from jpc_wifi_notnull_rule temp1 join jpc_wifi_notnull_test temp2 on temp1.wifi_id=temp2.wifi_id) temp3;

drop table if exists jpc_wifi_notnull_test_result ;
create table if not exists jpc_wifi_notnull_test_result as
select row_id,shop_id from jpc_wifi_notnull_test_temp where shop_rank=1;

select count(distinct row_id) from  jpc_wifi_notnull_test;  -- 1519961
select count(*) from  jpc_wifi_notnull_test_result where shop_id is not null;   -- 1534627

--null wifi 按一个wifi被不同shop搜索到次数排序，统计的是每个wifi下不同shop的个数。
drop table if exists jpc_wifi_null_rule;
create table if not exists jpc_wifi_null_rule as
select wifi_id,shop_id,shop_num from (
select wifi_id,shop_id,shop_num, rank() over(partition by wifi_id order by shop_num desc) shop_rank from(
select shop_id,wifi_id,count(shop_id) shop_num from jpc_wifi_null_train group by wifi_id,shop_id)  temp) temp1 where shop_rank = 1 or shop_num > 10;

select count(distinct wifi_id) from jpc_wifi_null_rule; --290715 distinct wifi_id
select count(*) from jpc_wifi_null_rule; --345811

drop table if exists jpc_wifi_null_test_result;
create table if not exists jpc_wifi_null_test_result as
select row_id,shop_id from (
select row_id, shop_id,shop_num,rank() over(partition by row_id order by shop_num) shop_rank from(
select temp2.row_id,temp1.wifi_id,temp1.shop_id,temp1.shop_num from jpc_wifi_null_rule temp1 join jpc_wifi_null_test  temp2 on temp1.wifi_id=temp2.wifi_id) temp3) temp4 
where shop_rank = 1;
select count(distinct row_id) from jpc_wifi_null_test;   --826990
select count(*) from jpc_wifi_null_test_result where shop_id is not null; --845500

drop table if exists union_null_notnull_result;
create table if not exists union_null_notnull_result as
select * from prj_tc_231620_98365_yrdets.jpc_wifi_notnull_test_result a
union 
select * from prj_tc_231620_98365_yrdets.jpc_wifi_null_test_result b; 

--生成预测结果,这里对union的结果又进行了row_number是因为，row_id有重复项。
drop table if exists ant_tianchi_ccf_sl_predict;
create table if not exists ant_tianchi_ccf_sl_predict  as
select row_id,shop_id from (
select row_id,shop_id,row_number() over(partition by row_id order by shop_id desc) shop_rank from
(select temp2.row_id,temp1.shop_id from union_null_notnull_result temp1 right outer join test_row_id temp2
on temp1.row_id=temp2.row_id) temp ) temp1 where shop_rank=1;

select count(*) from  ant_tianchi_ccf_sl_predict;
select count(distinct row_id) from  union_null_notnull_result;
select count(distinct row_id) from ant_tianchi_ccf_sl_predict result where result.shop_id is null;
