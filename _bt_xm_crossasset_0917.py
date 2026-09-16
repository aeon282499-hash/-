"""建て時刻に既知のクロスアセット指標で現行3レッグを条件付け(未検証): USDJPY/EURUSD/GOLD/OIL/US500/JP225 の当日(建て時刻までの)変化率を三分位に分ける。
採用条件=単調・両半同符号・除外側が明確にマイナス。日経=15:00JST建て, US500=15:00, GER40=01:00。"""
import pandas as pd, numpy as np, pickle
src=open('_bt_xm_recheck_0917.py',encoding='utf-8').read().split("print('\n== A")[0]; exec(src)
for k,v in {'JP225Cash':3.5-1.8,'US500Cash':8.2-1.3,'GER40Cash':7.9}.items(): S[k]['swap_long']=v
LJ=legs('JP225Cash',15,9,[0,1,2,3],80); LU=legs('US500Cash',15,9,[0,1,2,3],80); LG=legs('GER40Cash',1,16,[1,2,3,4],26)
j=LJ[(LJ.prev<=0)|(LJ.dow==0)]; u=LU[(LU.prev<=0)|(LU.dow==0)]; g=LG[LG.prev<=0]
O={s:jst(H[s]).open for s in ['USDJPY','EURUSD','GOLD.','OILCash','US500Cash','JP225Cash','GER40Cash','US100Cash'] if s in H}
for f in ['_xm_more_idx_h1.pkl']:
    for k,v in pickle.load(open(f,'rb')).items():
        if k in ['US100Cash','HK50Cash']: O[k]=jst(v['h1']).open
def chg(sym,L,h_from,h_to,days_back=0):
    o=O[sym]; out=[]
    for d in L.index:
        t1=d+pd.Timedelta(hours=h_to); t0=d-pd.Timedelta(days=days_back)+pd.Timedelta(hours=h_from)
        a=o.get(t0); b=o.get(t1); out.append((b/a-1)*100 if (a and b) else np.nan)
    return pd.Series(out,index=L.index)
def tercile(L,x,lab):
    x=x.reindex(L.index); q=x.quantile([1/3,2/3]); rows=[]
    for nm,m in [('下',x<=q.iloc[0]),('中',(x>q.iloc[0])&(x<=q.iloc[1])),('上',x>q.iloc[1])]:
        n=L.net[m]; h=n.index.year<=2020; rows.append(f'{nm}{n.mean():+.3f}(t{n.mean()/n.std()*np.sqrt(len(n)):+.1f} 前{n[h].mean():+.2f}/後{n[~h].mean():+.2f})')
    print(f'  {lab:34s} '+' '.join(rows))
print('== 日経 15:00建て(前夜≤0 or 月曜)  基準:',stats(j.net))
tercile(j,chg('USDJPY',j,9,15),'USDJPY 9→15時')
tercile(j,chg('USDJPY',j,15,15,1),'USDJPY 前日15→当日15時')
tercile(j,chg('GOLD.',j,9,15),'GOLD 9→15時')
tercile(j,chg('OILCash',j,9,15),'OIL 9→15時')
tercile(j,chg('US500Cash',j,9,15),'US500 9→15時(アジア時間の米先物)')
tercile(j,chg('JP225Cash',j,9,15),'日経 当日9→15時(日中)')
tercile(j,chg('HK50Cash',j,10,15),'HK50 10→15時')
print('== US500 15:00建て  基準:',stats(u.net))
tercile(u,chg('USDJPY',u,9,15),'USDJPY 9→15時')
tercile(u,chg('EURUSD',u,9,15),'EURUSD 9→15時')
tercile(u,chg('GOLD.',u,9,15),'GOLD 9→15時')
tercile(u,chg('OILCash',u,9,15),'OIL 9→15時')
tercile(u,chg('JP225Cash',u,9,15),'日経 9→15時')
tercile(u,chg('US500Cash',u,9,15),'US500 9→15時(自身のアジア時間)')
tercile(u,chg('US100Cash',u,15,15,1),'US100 前日15→当日15時')
print('== GER40 01:00建て(直前レッグ≤0)  基準:',stats(g.net))
tercile(g,chg('EURUSD',g,16,25),'EURUSD 16→翌01時')
tercile(g,chg('US500Cash',g,16,25),'US500 16→01時(欧州時間の米)')
tercile(g,chg('GER40Cash',g,16,25),'GER40 自身16→01時(欧州日中)')
tercile(g,chg('OILCash',g,16,25),'OIL 16→01時')
tercile(g,chg('GOLD.',g,16,25),'GOLD 16→01時')
tercile(g,chg('JP225Cash',g,15,25),'日経 15→01時')
