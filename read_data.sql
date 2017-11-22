create table if not exists ant_tianchi_ccf_sl_shop_info as select * from odps_tc_257100_f673506e024.ant_tianchi_ccf_sl_shop_info;
create table if not exists ant_tianchi_ccf_sl_user_shop_behavior as select * from odps_tc_257100_f673506e024.ant_tianchi_ccf_sl_user_shop_behavior;
create table if not exists ant_tianchi_ccf_sl_test as select * from odps_tc_257100_f673506e024.ant_tianchi_ccf_sl_test;
create table merge_train as
select e1.shop_id,e1.mall_id,e2.user_id,e2.time_stamp,e2.longitude,e2.latitude,e2.wifi_infos from prj_tc_231620_98365_yrdets.ant_tianchi_ccf_sl_shop_info  as e1 join prj_tc_231620_98365_yrdets.ant_tianchi_ccf_sl_user_shop_behavior  as e2
on e1.shop_id = e2.shop_id;
