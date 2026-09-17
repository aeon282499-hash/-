import pandas as pd, pickle
d=pickle.load(open('_xm_stocks_all_h1.pkl','rb')); print('銘柄数',len(d))
rows=[]
for k,v in d.items():
    h=v['h1']; t=pd.to_datetime(h['time'],unit='s'); sp=v['spec']; parts=sp['path'].split(chr(92))
    rows.append((k,parts[1],parts[2] if len(parts)>2 else '',len(h),t.min().date(),t.max().date(),sp['bid'],sp['spread'],sp['swap_long'],sp['swap_short'],sp['swap_mode'],sp['tmode']))
df=pd.DataFrame(rows,columns=['sym','reg','sub','n','from','to','bid','spread','swl','sws','swm','tmode'])
print(df.groupby('reg').agg(n=('sym','size'),bars_med=('n','median'),bid0=('bid',lambda x:(x==0).sum())).to_string())
print(df.groupby('reg')['from'].apply(lambda s: s.astype(str).str[:4].value_counts().sort_index().to_dict()).to_string())
df.to_csv('_xm_stocks_all_summary_0917.csv',index=False)
