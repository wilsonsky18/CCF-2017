-- 第一次分割，将多个wifi分割后合并到一列，按 ; 分割
DROP TABLE IF EXISTS temp_test;
CREATE TABLE temp_test
AS
SELECT user_id, row_id, mall_id, time_stamp, longitude, latitude
	, split_part(wifi_infos, ';', 1) AS wifi
FROM (
	SELECT *
	FROM ant_tianchi_ccf_sl_test 
) wifi_new
UNION
SELECT user_id, row_id, mall_id, time_stamp, longitude, latitude
	, split_part(wifi_infos, ';', 2) AS wifi
FROM (
	SELECT *
	FROM ant_tianchi_ccf_sl_test
) wifi_new
UNION
SELECT user_id, row_id, mall_id, time_stamp, longitude, latitude
	, split_part(wifi_infos, ';', 3) AS wifi
FROM (
	SELECT *
	FROM ant_tianchi_ccf_sl_test
) wifi_new
UNION
SELECT user_id, row_id, mall_id, time_stamp, longitude, latitude
	, split_part(wifi_infos, ';', 4) AS wifi
FROM (
	SELECT *
	FROM ant_tianchi_ccf_sl_test
) wifi_new
UNION
SELECT user_id, row_id, mall_id, time_stamp, longitude, latitude
	, split_part(wifi_infos, ';', 5) AS wifi
FROM (
	SELECT *
	FROM ant_tianchi_ccf_sl_test
) wifi_new
UNION
SELECT user_id, row_id, mall_id, time_stamp, longitude, latitude
	, split_part(wifi_infos, ';', 6) AS wifi
FROM (
	SELECT *
	FROM ant_tianchi_ccf_sl_test
) wifi_new
UNION
SELECT user_id, row_id, mall_id, time_stamp, longitude, latitude
	, split_part(wifi_infos, ';', 7) AS wifi
FROM (
	SELECT *
	FROM ant_tianchi_ccf_sl_test
) wifi_new
UNION
SELECT user_id, row_id, mall_id, time_stamp, longitude, latitude
	, split_part(wifi_infos, ';', 8) AS wifi
FROM (
	SELECT *
	FROM ant_tianchi_ccf_sl_test
) wifi_new
UNION
SELECT user_id, row_id, mall_id, time_stamp, longitude, latitude
	, split_part(wifi_infos, ';', 9) AS wifi
FROM (
	SELECT *
	FROM ant_tianchi_ccf_sl_test
) wifi_new
UNION
SELECT user_id, row_id, mall_id, time_stamp, longitude, latitude
	, split_part(wifi_infos, ';', 10) AS wifi
FROM (
	SELECT *
	FROM ant_tianchi_ccf_sl_test
) wifi_new;
SELECT user_id, row_id, mall_id, time_stamp, longitude, latitude
	, split_part(wifi_infos, ';', 11) AS wifi
FROM (
	SELECT *
	FROM ant_tianchi_ccf_sl_test
) wifi_new
UNION
SELECT user_id, row_id, mall_id, time_stamp, longitude, latitude
	, split_part(wifi_infos, ';', 12) AS wifi
FROM (
	SELECT *
	FROM ant_tianchi_ccf_sl_test
) wifi_new
UNION
SELECT user_id, row_id, mall_id, time_stamp, longitude, latitude
	, split_part(wifi_infos, ';', 13) AS wifi
FROM (
	SELECT *
	FROM ant_tianchi_ccf_sl_test
) wifi_new
UNION
SELECT user_id, row_id, mall_id, time_stamp, longitude, latitude
	, split_part(wifi_infos, ';', 14) AS wifi
FROM (
	SELECT *
	FROM ant_tianchi_ccf_sl_test
) wifi_new
UNION
SELECT user_id, row_id, mall_id, time_stamp, longitude, latitude
	, split_part(wifi_infos, ';', 15) AS wifi
FROM (
	SELECT *
	FROM ant_tianchi_ccf_sl_test
) wifi_new
UNION
SELECT user_id, row_id, mall_id, time_stamp, longitude, latitude
	, split_part(wifi_infos, ';', 16) AS wifi
FROM (
	SELECT *
	FROM ant_tianchi_ccf_sl_test
) wifi_new
UNION
SELECT user_id, row_id, mall_id, time_stamp, longitude, latitude
	, split_part(wifi_infos, ';', 17) AS wifi
FROM (
	SELECT *
	FROM ant_tianchi_ccf_sl_test
) wifi_new
UNION
SELECT user_id, row_id, mall_id, time_stamp, longitude, latitude
	, split_part(wifi_infos, ';',18) AS wifi
FROM (
	SELECT *
	FROM ant_tianchi_ccf_sl_test
) wifi_new
UNION
SELECT user_id, row_id, mall_id, time_stamp, longitude, latitude
	, split_part(wifi_infos, ';', 19) AS wifi
FROM (
	SELECT *
	FROM ant_tianchi_ccf_sl_test
) wifi_new
UNION
SELECT user_id, row_id, mall_id, time_stamp, longitude, latitude
	, split_part(wifi_infos, ';', 20) AS wifi
FROM (
	SELECT *
	FROM ant_tianchi_ccf_sl_test
) wifi_new;

-- 第二次分割，按 | 分割
DROP TABLE IF EXISTS jpc_test;
CREATE TABLE jpc_test
AS
SELECT user_id, row_id, mall_id, time_stamp, longitude, latitude
	, split_part(wifi, '|', 1) AS wifi_id
	, split_part(wifi, '|', 2) AS signal
	, split_part(wifi, '|', 3) AS connect
FROM (
	SELECT *
	FROM temp_test
	WHERE wifi IS NOT NULL
		and wifi != ''
) e;


-- join the trian to wifi_filter
DROP TABLE IF EXISTS jpc_second_test;
CREATE TABLE IF NOT EXISTS jpc_second_test
AS
SELECT temp1.user_id, temp1.mall_id, temp1.row_id, temp1.longitude, temp1.latitude
	, temp1.wifi_id, temp1.signal, temp1.connect
FROM jpc_test temp1
JOIN jpc_wifi_filter temp2
ON temp1.wifi_id = temp2.wifi_id
