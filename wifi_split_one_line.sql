DROP TABLE IF EXISTS wifi_split_train;
CREATE TABLE if not exists wifi_split_train AS
SELECT user_id, shop_id, mall_id, time_stamp, longitude, latitude,
split_part(wifi_infos, ';', 1) wifi1,split_part(wifi_infos, ';', 2) wifi2,split_part(wifi_infos, ';', 3) wifi3,split_part(wifi_infos, ';', 4) wifi4,split_part(wifi_infos, ';', 5) wifi5,
split_part(wifi_infos, ';', 6) wifi6,split_part(wifi_infos, ';', 7) wifi7,split_part(wifi_infos, ';', 8) wifi8,split_part(wifi_infos, ';', 9) wifi9,split_part(wifi_infos, ';', 10) wifi10,
split_part(wifi_infos, ';', 11) wifi11,split_part(wifi_infos, ';', 12) wifi12,split_part(wifi_infos, ';', 13) wifi13,split_part(wifi_infos, ';', 14) wifi14,split_part(wifi_infos, ';', 15)wifi15,
split_part(wifi_infos, ';', 16) wifi16,split_part(wifi_infos, ';', 17) wifi17,split_part(wifi_infos, ';', 18) wifi18,split_part(wifi_infos, ';', 19) wifi19,split_part(wifi_infos, ';', 20) wifi20
from merge_train;

DROP TABLE IF EXISTS wifi_split_train;
CREATE TABLE jpc_train
AS
SELECT user_id, shop_id, mall_id, time_stamp, longitude, latitude
	, split_part(wifi1, '|', 1) AS wifi_id1
	, split_part(wifi1, '|', 2) AS signal1
	, split_part(wifi1, '|', 3) AS connect1
	, split_part(wifi2, '|', 1) AS wifi_id2
	, split_part(wifi2, '|', 2) AS signal2
	, split_part(wifi2, '|', 3) AS connect2
	, split_part(wifi3, '|', 1) AS wifi_id3
	, split_part(wifi3, '|', 2) AS signal3
	, split_part(wifi3, '|', 3) AS connect3
	, split_part(wifi4, '|', 1) AS wifi_id4
	, split_part(wifi4, '|', 2) AS signal4
	, split_part(wifi4, '|', 3) AS connect4
	, split_part(wifi5, '|', 1) AS wifi_id5
	, split_part(wifi5, '|', 2) AS signal5
	, split_part(wifi5, '|', 3) AS connect5
	, split_part(wifi6, '|', 1) AS wifi_id6
	, split_part(wifi6, '|', 2) AS signal6
	, split_part(wifi6, '|', 3) AS connect6
	, split_part(wifi7, '|', 1) AS wifi_id7
	, split_part(wifi7, '|', 2) AS signal7
	, split_part(wifi7, '|', 3) AS connect7
	, split_part(wifi8, '|', 1) AS wifi_id8
	, split_part(wifi8, '|', 2) AS signal8
	, split_part(wifi8, '|', 3) AS connect8
	, split_part(wifi9, '|', 1) AS wifi_id9
	, split_part(wifi9, '|', 2) AS signal9
	, split_part(wifi9, '|', 3) AS connect9
	, split_part(wifi10, '|', 1) AS wifi_id10
	, split_part(wifi10, '|', 2) AS signal10
	, split_part(wifi10, '|', 3) AS connect10
FROM (
	SELECT *
	FROM wifi_split_train
) 
