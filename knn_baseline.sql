CREATE TABLE IF NOT EXISTS lon_la_shop_info
AS
SELECT longitude, latitude, mall_id
FROM prj_tc_231620_98365_yrdets.ant_tianchi_ccf_sl_shop_info;
ALTER TABLE lon_la_shop_info CHANGE
longitude shop_longitude double 
latitude shop_latitude double
