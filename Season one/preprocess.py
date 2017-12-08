#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 11 16:52:34 2017

@author: lusir
"""

import pandas as pd
import numpy as np

train = pd.read_csv('ccf_first_round_user_shop_behavior.csv')
test = pd.read_csv('A_evaluation_public.csv')
shop = pd.read_csv('ccf_shop_info.csv')

ctrain = train.copy()
ctest = test.copy()

cshop = shop.loc[:,['shop_id','mall_id']]
ctrain = pd.merge(ctrain,cshop,on='shop_id')

shop.shop_id = shop.shop_id.str.split('_').str[1]
shop.mall_id = shop.mall_id.str.split('_').str[1]
#train_shop_id
ctrain['shop_id'] = ctrain['shop_id'].str.split('_').str[1]
ctrain['shop_id'] = ctrain['shop_id'].astype(int)
ctrain['mall_id'] = ctrain['mall_id'].str.split('_').str[1]
ctrain['mall_id'] = ctrain['mall_id'].astype(int)

#test_mall_id
ctest['mall_id'] = ctest['mall_id'].str.split('_').str[1]
ctest['mall_id'] = ctest['mall_id'].astype(int)

#one mall subdataset
ctrain = ctrain[ctrain.mall_id == 7800]
ctest = ctest[ctest.mall_id == 7800]

def data_preprocess(ctrain,ctest):
    for data in [ctrain,ctest]:
        data['user_id'] = data['user_id'].str.split('_').str[1]
        
        #wifi_split
        for i in range(0,10):
            data['w_info'+str(i+1)] = data['wifi_infos'].str.split(';').str[i]
        data.drop(['wifi_infos'],axis=1,inplace=True)
        for j in range(1,11):
            data['info'+str(j)+'_id'] = data['w_info'+str(j)].str.split('|').str[0]
            data['info'+str(j)+'_id'] = data['info'+str(j)+'_id'].str.split('_').str[1]
            data['info'+str(j)+'_id'] = data['info'+str(j)+'_id'].fillna(0)
            data['info'+str(j)+'_id'] = data['info'+str(j)+'_id'].astype(np.int64)
            
            data['info'+str(j)+'_signal'] = data['w_info'+str(j)].str.split('|').str[1]
            data['info'+str(j)+'_signal'] = data['info'+str(j)+'_signal'].fillna(-100)
            data['info'+str(j)+'_signal'] = data['info'+str(j)+'_signal'].astype(np.int64)
            
            data['info'+str(j)+'_flag'] = data['w_info'+str(j)].str.split('|').str[2]
            data['info'+str(j)+'_flag'] = data['info'+str(j)+'_flag'].fillna('false')
            data['info'+str(j)+'_signal'] = data['info'+str(j)+'_signal'].astype(str)
            
            data.drop(['w_info'+str(j)],axis=1,inplace=True)
        
        #time_split
        data['time_stamp'] = data['time_stamp'].str.split().str[1]
        data['time_stamp_hour'] = data['time_stamp'].str.split(':').str[0]
        data['time_stamp_min'] = data['time_stamp'].str.split(':').str[1]
        data['time_stamp_hour'] = data['time_stamp_hour'].astype(int)
        data['time_stamp_min'] = data['time_stamp_min'].astype(int)
        data['time_stamp'] = data['time_stamp_hour']*60 + data['time_stamp_min']
        data.drop(['time_stamp_min','time_stamp_hour'],axis=1,inplace=True)

        
    return ctrain,ctest



ctrain,ctest = data_preprocess(ctrain,ctest)

ctrain.to_csv('train7800.csv',index=False)
ctest.to_csv('test7800.csv',index=False)
shop.to_csv('shop_info.csv',index=False)
