import pandas as pd
from sklearn.ensemble import RandomForestClassifier

shop_info=pd.read_csv('训练数据-ccf_first_round_shop_info.csv')
mall_list=list(set(list(shop_info.mall_id)))
print(mall_list)
for mall in mall_list:
    data_mall = pd.read_csv('~/Tianchi/treated_data/'+mall+'.csv')
    train_mall = data_mall[data_mall['shop_id'].notnull()]
    test_mall = data_mall[data_mall['shop_id'].isnull()]
    #生成概率输出
    candidate_list = ['cls1','prob1','cls2','prob2','cls3','prob3','cls4','prob4','cls5','prob5']
    for x in candidate_list: test_mall[x] = 0.0
    count = 0
    shop_list = list(set(train_mall['shop_id']))
    for shop in shop_list:
        train_pos = train_mall[train_mall['shop_id'] == shop]
        train_pos = train_pos.dropna(axis=1,how='all') #删除正样本中不出现的wifi
        features = [x for x in train_pos.columns if x not in ['user_id', 'shop_id', 'mall_id', 'wifi_infos', 'time_stamp']]
        train_neg = train_mall[train_mall['shop_id'] != shop][features] #负样本
        train_pos['label'] = 1
        train_neg['label'] = 0
        train = pd.concat([train_pos,train_neg])
        clf_rf = RandomForestClassifier(n_estimators=100,oob_score=True)
        clf_rf.fit(train[features].fillna(-100),train['label'])
        pos_prob = clf_rf.predict_proba(test_mall[features].fillna(-100))[:,1] #取出概率最高的
        test_mall[shop] = pos_prob #总集合,在此基础上生成候选集
        count += 1
        print(count)
    mall_result = []
    for index, row in test_mall.iterrows():
        temp = row[shop_list].sort_values(ascending=False) #排序,取前五为候选集
        row[[candidate_list[i] for i in [0, 2, 4, 6, 8]]] = temp.index.values[0:5]
        row[[candidate_list[i] for i in [1, 3, 5, 7, 9]]] = temp.values[0:5]
        mall_result.append(row[['row_id']+candidate_list])
    mall_result = pd.DataFrame(mall_result)
    mall_result['row_id'] = mall_result['row_id'].astype('int')
    mall_result.to_csv('binary_rf.csv', index=False)
