--计算概率总和
drop table if exists m_1309_prob;
create table if not exists m_1309_prob as
select row_id, shop_id, SUM( prediction_score ) prob
from m_1309_result 
group by shop_id,row_id;

--选出概率最大的商店
drop table if exists temp_1309_result;
create table if not exists temp_1309_result_2 as
select row_id,shop_id from(  
select row_id, shop_id, prob, row_number () over (distribute by row_id sort by prob desc) a  from m_1309_prob) b where b.a = 1;


--rank函数出现重复项，为解决
drop table if exists temp_1309_result;
create table if not exists temp_1309_result as
select row_id,shop_id from (
select row_id,shop_id,prob_rank from (
select  row_id, shop_id, prob, rank() over (distribute by row_id sort by prob desc ) as prob_rank from m_1309_prob) a where prob_rank=1) b group by row_id,shop_id;
