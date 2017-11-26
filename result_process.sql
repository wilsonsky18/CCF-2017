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
