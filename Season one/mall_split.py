# -*- coding: utf-8 -*-
"""
Created on Thu Oct 26 20:25:14 2017

@author: JPC
"""

import pandas as pd
import numpy as np

train = pd.read_csv('train.csv')
shop_info = pd.read_csv('shop_info.csv')
test = pd.read_csv('test.csv')

def data_split(train,test,shop_info,split=1):
    mall_unique = list(np.unique(shop_info.mall_id))
    num = int(97/split)
    temp = 0
    for i in range(num):
        split_mall = mall_unique[temp:temp+split]
        
        split_train = train.iloc[[0]]
        split_test = test.iloc[[0]]
        split_shop = shop_info.iloc[[0]]
        for index in split_mall:
            temp_train = train[train.mall_id == index]
            temp_test = test[test.mall_id == index]
            temp_shop = shop_info[shop_info.mall_id == index]
            
            split_train = split_train.append(temp_train,ignore_index=True)
            split_test = split_test.append(temp_test,ignore_index=True)
            split_shop = split_shop.append(temp_shop,ignore_index=True)
        
        split_train.drop(0,inplace=True)
        split_test.drop(0,inplace=True)
        split_shop.drop(0,inplace=True)
        
        split_train.to_csv('train_'+str(i)+'.csv',index=False)
        split_test.to_csv('test_'+str(i)+'.csv',index=False)
        split_shop.to_csv('shop_'+str(i)+'.csv',index=False)
                
        if (temp + split) > 97:
            split = 97 - temp
        else:
            temp += split

    return split_train,split_test,split_shop

split_train,split_test,split_shop = data_split(train,test,shop_info,split=1)
