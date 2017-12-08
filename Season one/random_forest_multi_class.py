# -*- coding: utf-8 -*-
"""
Created on Thu Oct 26 20:25:14 2017

@author: JPC
"""
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
import os

ctrain,ctest,cshop,cresult=[],[],[],[]

for m in range(0,97):
#for m in []:
    ctrain.append('train_'+str(m)+'.csv')
    ctest.append('test_'+str(m)+'.csv')
    cshop.append('shop_'+str(m)+'.csv')



for k in range(97):
    print('Reading data from files')
    shop_behavior = pd.read_csv(ctrain[k])
    shop_info = pd.read_csv(cshop[k])
    test = pd.read_csv(ctest[k])
    print('Original data has been read')

    #file size confirm
    #fsize = os.path.getsize(ctrain[k])/1024
    #if fsize < 4500:
    #    continue

    # 获取商场id集合
    data_mall_id = np.array(shop_info['mall_id'])
    mall_id = set(data_mall_id)
    # 创建字典,键为商场id（数值型),值为商铺id
    mallid_to_shopid = dict()
    print('Start to create the classifiers')
    for i in mall_id:
        sub_dataframe_info = shop_info[shop_info['mall_id'].isin([i])]
        sub_shopid = np.array(sub_dataframe_info['shop_id'])
        mallid_to_shopid[i] = sub_shopid
    # 创建字典,键为商场id,值为子数据集
    mallid_to_subdataframe = dict()
    # 创建字典,键为商场id,值为RF分类器
    mallid_to_RF = dict()
    j = 0 # 创建分类器的进度指示器
    def pretreat_train(X): # 将数据集转换为NB输入格式的矩阵,同时返回wifiid到矩阵列号的字典
        dataframe_wifiid = X[['info1_id', 'info2_id', 'info3_id', 'info4_id', 'info5_id', 'info6_id', 'info7_id', 'info8_id', 'info9_id','info10_id']]
        uniqueWifiid = list(np.unique(np.array(dataframe_wifiid)))
        uniqueWifiid = [value for value in uniqueWifiid if value != 0]  # 去除空值
        array_wifi = np.ones((len(X), len(uniqueWifiid))) * (-100)
        # 建立字典,从wifiID映射到矩阵列号
        wifiid_to_column = dict()
        for index, value in enumerate(uniqueWifiid):
            wifiid_to_column[value] = index
        # 遍历子数据集数据框信息,完成矩阵填充
        X = X.reset_index()
        for index, row in X.iterrows():
            if row['info1_id'] != 0:
                array_wifi[index, wifiid_to_column[row['info1_id']]] = row['info1_signal']
                if row['info2_id'] != 0:
                    array_wifi[index, wifiid_to_column[row['info2_id']]] = row['info2_signal']
                    if row['info3_id'] != 0:
                        array_wifi[index, wifiid_to_column[row['info3_id']]] = row['info3_signal']
                        if row['info4_id'] != 0:
                            array_wifi[index, wifiid_to_column[row['info4_id']]] = row['info4_signal']
                            if row['info5_id'] != 0:
                                array_wifi[index, wifiid_to_column[row['info5_id']]] = row['info5_signal']
                                if row['info6_id'] != 0:
                                    array_wifi[index, wifiid_to_column[row['info6_id']]] = row['info6_signal']
                                    if row['info7_id'] != 0:
                                        array_wifi[index, wifiid_to_column[row['info7_id']]] = row['info7_signal']
                                        if row['info8_id'] != 0:
                                            array_wifi[index, wifiid_to_column[row['info8_id']]] = row['info8_signal']
                                            if row['info9_id'] != 0:
                                                array_wifi[index, wifiid_to_column[row['info9_id']]] = row['info9_signal']
                                                if row['info10_id'] != 0:
                                                    array_wifi[index, wifiid_to_column[row['info10_id']]] = row['info10_signal']
        return (array_wifi,wifiid_to_column)
    # 创建字典,键为商场id,值为wifiid_to_column
    mallid__to__wifiid_to_column = dict()
    # 对每个商场建立模型
    for i in mall_id:
        sub_dataframe_behavior = shop_behavior[shop_behavior['shop_id'].isin(mallid_to_shopid[i])]
        #mallid_to_subdataframe[i] = sub_dataframe_behavior
        (X_train,wifiid_to_column) = pretreat_train(sub_dataframe_behavior)
        mallid__to__wifiid_to_column[i] = wifiid_to_column
        y_train = np.array(sub_dataframe_behavior['shop_id'])

        #add Non-wifi features
        X_train = pd.DataFrame(X_train)
        X_train['longitude'] = sub_dataframe_behavior.longitude
        X_train['latitude'] = sub_dataframe_behavior.latitude
        X_train = np.array(X_train)

        rf = RandomForestClassifier(bootstrap=True, class_weight=None, criterion='gini',
            max_depth=120, max_features='sqrt', max_leaf_nodes=None,
            min_impurity_decrease=0.0, min_impurity_split=None,
            min_samples_leaf=1, min_samples_split=2,
            min_weight_fraction_leaf=0.0, n_estimators=1800, n_jobs=-1,
            oob_score=True, random_state=None, verbose=0,
            warm_start=False)
        rf.fit(X_train,y_train)
        mallid_to_RF[i] = rf
        j+=1
        print('已训练{0}个模型.'.format(j))

    # 下面开始预测
    shop_id_predict = list()
    print('Start to predict')
    for index, row in test.iterrows():
        rf_instance = mallid_to_RF[row['mall_id']]
        wifiid_to_column_instance = mallid__to__wifiid_to_column[row['mall_id']]
        array_wifi_instance = np.ones((1,len(wifiid_to_column_instance)))*(-100)
        if row['info1_id'] != 0:
            if row['info1_id'] in wifiid_to_column_instance.keys(): array_wifi_instance[0, wifiid_to_column_instance[row['info1_id']]] = row['info1_signal']
            if row['info2_id'] != 0:
                if row['info2_id'] in wifiid_to_column_instance.keys(): array_wifi_instance[0, wifiid_to_column_instance[row['info2_id']]] = row['info2_signal']
                if row['info3_id'] != 0:
                    if row['info3_id'] in wifiid_to_column_instance.keys(): array_wifi_instance[0, wifiid_to_column_instance[row['info3_id']]] = row['info3_signal']
                    if row['info4_id'] != 0:
                        if row['info4_id'] in wifiid_to_column_instance.keys(): array_wifi_instance[0, wifiid_to_column_instance[row['info4_id']]] = row['info4_signal']
                        if row['info5_id'] != 0:
                            if row['info5_id'] in wifiid_to_column_instance.keys(): array_wifi_instance[0, wifiid_to_column_instance[row['info5_id']]] = row['info5_signal']
                            if row['info6_id'] != 0:
                                if row['info6_id'] in wifiid_to_column_instance.keys(): array_wifi_instance[0, wifiid_to_column_instance[row['info6_id']]] = row['info6_signal']
                                if row['info7_id'] != 0:
                                    if row['info7_id'] in wifiid_to_column_instance.keys(): array_wifi_instance[0, wifiid_to_column_instance[row['info7_id']]] = row['info7_signal']
                                    if row['info8_id'] != 0:
                                        if row['info8_id'] in wifiid_to_column_instance.keys(): array_wifi_instance[0, wifiid_to_column_instance[row['info8_id']]] = row['info8_signal']
                                        if row['info9_id'] != 0:
                                            if row['info9_id'] in wifiid_to_column_instance.keys(): array_wifi_instance[0, wifiid_to_column_instance[row['info9_id']]] = row['info9_signal']
                                            if row['info10_id'] != 0:
                                                if row['info10_id'] in wifiid_to_column_instance.keys(): array_wifi_instance[0, wifiid_to_column_instance[row['info10_id']]] = row['info10_signal']
        X_instance = array_wifi_instance.reshape(1,-1)

        X_instance = pd.DataFrame(X_instance)
        X_instance['longitude'],X_instance['latitude'] = row['longitude'],row['latitude']
        X_instance = np.array(X_instance)

        shop_id_predict.append((rf_instance.predict(X_instance))[0])
        if index % 1000 == 0:
            print('已预测{0}条数据.'.format(index))
    # 数据逆清洗
    def addstr(x):
        return 's_'+str(x)
    print('完成预测,开始逆清洗数据.')
    shop_id_predict=list(map(addstr,shop_id_predict))
    # 数据整合存档
    row_id = list(np.array(test['row_id']))
    result_array = np.array([row_id,shop_id_predict]).transpose()
    result_frame = pd.DataFrame(result_array,columns=['row_id','shop_id'])
    print('Ready to write.')
    result_frame.to_csv(path_or_buf='result'+str(ctrain[k][6:8])+'.csv',index=False)
    print('result'+str(ctrain[k][6:8])+' Writing is done.')
