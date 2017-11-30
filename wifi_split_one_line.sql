DROP TABLE IF EXISTS wifi_split_train;
CREATE TABLE if not exists wifi_split_train AS
SELECT user_id, shop_id, mall_id, time_stamp, longitude, latitude,
split_part(wifi_infos, ';', 1) wifi1,split_part(wifi_infos, ';', 2) wifi2,split_part(wifi_infos, ';', 3) wifi3,split_part(wifi_infos, ';', 4) wifi4,split_part(wifi_infos, ';', 5) wifi5,
split_part(wifi_infos, ';', 6) wifi6,split_part(wifi_infos, ';', 7) wifi7,split_part(wifi_infos, ';', 8) wifi8,split_part(wifi_infos, ';', 9) wifi9,split_part(wifi_infos, ';', 10) wifi10,
split_part(wifi_infos, ';', 11) wifi11,split_part(wifi_infos, ';', 12) wifi12,split_part(wifi_infos, ';', 13) wifi13,split_part(wifi_infos, ';', 14) wifi14,split_part(wifi_infos, ';', 15)wifi15,
split_part(wifi_infos, ';', 16) wifi16,split_part(wifi_infos, ';', 17) wifi17,split_part(wifi_infos, ';', 18) wifi18,split_part(wifi_infos, ';', 19) wifi19,split_part(wifi_infos, ';', 20) wifi20
from merge_train;
