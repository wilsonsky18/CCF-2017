import pandas as pd

df=pd.read_csv('~/Tianchi/训练数据-ccf_first_round_user_shop_behavior.csv')
shop_info=pd.read_csv('~/Tianchi/训练数据-ccf_first_round_shop_info.csv')
test=pd.read_csv('~/Tianchi/AB榜测试集-evaluation_public.csv')
df=pd.merge(df,shop_info[['shop_id','mall_id']],how='left',on='shop_id')
train=pd.concat([df,test])
train['time_stamp']=pd.to_datetime(train['time_stamp'])
# train['time_minutes'] = train['time_stamp'].apply(lambda x: x.hour * 60 + x.minute)
# train['time_weekday'] = train['time_stamp'].apply(lambda x: x.dayofweek)
print('Time has been preprocessed.')
mall_list=list(set(list(shop_info.mall_id)))
result=pd.DataFrame()
mall_list.sort()
for mall in mall_list:
    train_mall = train[train['mall_id'] == mall].reset_index(drop=True)
    l = []
    for index, row in train_mall.iterrows():
        wifi_list = [wifi.split('|') for wifi in row['wifi_infos'].split(';')]
        wifi_dict = {}
        for i in wifi_list:
            row[i[0]] = int(i[1])  # 列索引为ID,值为强度
            # if row[i[2]] == 'true': row['wifi_con'] = str(row[i[0]])  # 连上的wifi_id
            if i[0] not in wifi_dict:
                wifi_dict[i[0]] = 1  # 计数,以过滤wifi
            else:
                wifi_dict[i[0]] += 1
        l.append(row)
    delate_wifi = []
    for i in wifi_dict:
        if wifi_dict[i] < 15:
            delate_wifi.append(i)
    m = []
    for row in l:
        new = {}
        for n in row.keys():
            if n not in delate_wifi:
                new[n] = row[n]
        m.append(new)
    train_mall = pd.DataFrame(m)
    train_mall.to_csv('~/Tianchi/treated_data/'+mall+'.csv', index = False)
