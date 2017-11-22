CREATE TABLE IF NOT EXISTS ant_tianchi_ccf_sl_shop_info
AS
SELECT *
FROM odps_tc_257100_f673506e024.ant_tianchi_ccf_sl_shop_info;

CREATE TABLE IF NOT EXISTS ant_tianchi_ccf_sl_user_shop_behavior
AS
SELECT *
FROM odps_tc_257100_f673506e024.ant_tianchi_ccf_sl_user_shop_behavior;

CREATE TABLE IF NOT EXISTS ant_tianchi_ccf_sl_test
AS
SELECT *
FROM odps_tc_257100_f673506e024.ant_tianchi_ccf_sl_test;

CREATE TABLE IF NOT EXISTS merge_train
AS
SELECT e1.shop_id, e1.mall_id, e2.user_id, e2.longitude, e2.latitude
	, e2.wifi_infos, e2.time_stamp AS time_stamp
FROM prj_tc_231620_98365_yrdets.ant_tianchi_ccf_sl_shop_info e1
JOIN prj_tc_231620_98365_yrdets.ant_tianchi_ccf_sl_user_shop_behavior e2
ON e1.shop_id = e2.shop_id;

CREATE TABLE IF NOT EXISTS lon_la_shop_info
AS
SELECT longitude, latitude, mall_id
FROM prj_tc_231620_98365_yrdets.ant_tianchi_ccf_sl_shop_info;
ALTER TABLE lon_la_shop_info CHANGE
longitude shop_longitude double 
latitude shop_latitude double

CREATE TABLE IF NOT EXISTS jpc_knn_train
AS
SELECT temp1.shop_longitude, temp1.shop_latitude, temp1.mall_id, temp2.shop_id, temp2.longitude
	, temp2.latitude
FROM prj_tc_231620_98365_yrdets.lon_la_shop_info temp1
JOIN prj_tc_231620_98365_yrdets.merge_train temp2
ON temp1.mall_id = temp2.mall_id;

CREATE TABLE IF NOT EXISTS jpc_knn_test
AS
SELECT temp1.shop_longitude, temp1.shop_latitude, temp1.mall_id, temp2.row_id, temp2.longitude
	, temp2.latitude
FROM prj_tc_231620_98365_yrdets.lon_la_shop_info temp1
JOIN prj_tc_231620_98365_yrdets.ant_tianchi_ccf_sl_test temp2
ON temp1.mall_id = temp2.mall_id;
