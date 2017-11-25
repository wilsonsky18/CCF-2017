--计算概率总和
drop table if exists m_1309_prob;
create table if not exists m_1309_prob as
select row_id, shop_id, SUM( prediction_score ) prob
from prj_tc_231620_98365_yrdets.m_1309_result 
group by shop_id,row_id;

--选出概率最大的商店
drop table if exists temp_1309_result;
create table if not exists temp_1309_result as
select row_id,shop_id from (
select row_id,shop_id,prob_rank from (
select  row_id, shop_id, prob, rank() OVER (partition by row_id order by prob desc ) as prob_rank from m_1309_prob) a where prob_rank=1) b;
select count(*) from temp_1309_result; 
select count(distinct row_id) from prj_tc_231620_98365_yrdets.test_m_1309 ; 
