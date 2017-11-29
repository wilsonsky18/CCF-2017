drop table if exists m_1060_wifi;
create table if not exists m_1060_wifi as
select wifi_id, shop_id, count(signal) num from(
select user_id, shop_id, wifi_id,signal from(select user_id, shop_id, wifi_id,signal, rank () over (distribute by user_id,shop_id sort by signal asc) a  from train_m_1006) b 
where b.a=1) c group by wifi_id,shop_id;

drop table if exists a_jpc_wifi ;
create table if not exists a_jpc_wifi as
select wifi_id, shop_id, count(signal) num from(
select user_id, shop_id, wifi_id,signal from(select user_id, shop_id, wifi_id,signal, rank () over (distribute by user_id,shop_id sort by signal asc) a  from jpc_second_train) b 
where b.a=1) c group by wifi_id,shop_id;
select count(*) from a_jpc_wifi;
select count(distinct wifi_id) from a_jpc_wifi;
