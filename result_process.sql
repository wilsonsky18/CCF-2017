drop table if exists m_1309_prob;
create table if not exists m_1309_prob as
SELECT row_id, shop_id, SUM( prediction_score ) AS prob
FROM prj_tc_231620_98365_yrdets.m_1309_result 
GROUP BY shop_id,row_id;

drop table if exists temp_1309_result;
create table if not exists temp_1309_result as
select row_id,max(prob) as prob from m_1309_prob
group by row_id；

drop table if exists temp_1309_result;
create table if not exists temp_1309_result as
select row_id，shop_id from m_1309_prob where prob in(select max(prob) as prob from m_1309_prob) group by row_id,shop_id;
