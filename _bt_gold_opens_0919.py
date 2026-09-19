"""9/19 本人「5分ショートでもいい・韓国/日経/他市場が空く時は？5分10分15分」→ 金10年M1で各市場の寄り/引け(各市場のDST込み)前後の5/10/15/30分×売買を厳密判定。コスト往復0.005%。"""
import pandas as pd, numpy as np
m1=pd.read_pickle('_fx_xauusd_m1.pkl'); o=m1.open; o.index=o.index.tz_localize('UTC')
EV=[('東京寄り09:00','Asia/Tokyo',9,0),('東京仲値09:55','Asia/Tokyo',9,55),('東京引け15:00','Asia/Tokyo',15,0),('大阪先物夜間16:30','Asia/Tokyo',16,30),
    ('韓国寄り09:00','Asia/Seoul',9,0),('韓国引け15:30','Asia/Seoul',15,30),
    ('上海金SGE寄り09:00','Asia/Shanghai',9,0),('上海株寄り09:30','Asia/Shanghai',9,30),('上海引け15:00','Asia/Shanghai',15,0),('SGE夜間20:00','Asia/Shanghai',20,0),
    ('香港寄り09:30','Asia/Hong_Kong',9,30),('香港引け16:00','Asia/Hong_Kong',16,0),('シンガポール09:00','Asia/Singapore',9,0),('シドニー10:00','Australia/Sydney',10,0),
    ('インド寄り09:15','Asia/Kolkata',9,15),('ドバイDGCX07:00','Asia/Dubai',7,0),
    ('フランクフルト09:00','Europe/Berlin',9,0),('欧州引け17:30','Europe/Berlin',17,30),('ロンドン株08:00','Europe/London',8,0),('ロンドン株引け16:30','Europe/London',16,30),
    ('LBMA AM10:30','Europe/London',10,30),('LBMA PM15:00','Europe/London',15,0),
    ('COMEXピット08:20','America/New_York',8,20),('米指標08:30','America/New_York',8,30),('NY株寄り09:30','America/New_York',9,30),('COMEX清算13:30','America/New_York',13,30),('NY株引け16:00','America/New_York',16,0)]
WIN=[('前15→0',-15,0),('前10→0',-10,0),('前5→0',-5,0),('0→5',0,5),('0→10',0,10),('0→15',0,15),('0→30',0,30),('5→15',5,15),('5→30',5,30)]
days=pd.Series(pd.to_datetime(sorted(set(o.index.date)))); days=days[days.dt.dayofweek<5]
COST=0.005; rows=[]
for ev,tz,h,m in EV:
    loc=pd.DatetimeIndex([d.replace(hour=h,minute=m) for d in days]).tz_localize(tz,ambiguous='NaT',nonexistent='NaT'); loc=loc[loc.notna()]
    t0=loc.tz_convert('UTC')
    for wl,a,b in WIN:
        pa=o.reindex(t0+pd.Timedelta(minutes=a),method='ffill',tolerance=pd.Timedelta(minutes=3)); pb=o.reindex(t0+pd.Timedelta(minutes=b),method='ffill',tolerance=pd.Timedelta(minutes=3))
        r=pd.Series((pb.values/pa.values-1)*100,index=t0).dropna(); r=r[r!=0]
        if len(r)<1500: continue
        for side,s in [('買',1),('売',-1)]:
            v=r*s; n=len(v); mid=n//2; t=v.mean()/v.std()*np.sqrt(n); t1=v[:mid].mean()/v[:mid].std()*np.sqrt(mid); t2=v[mid:].mean()/v[mid:].std()*np.sqrt(n-mid)
            yr=v.groupby(v.index.year).mean(); rows.append(dict(ev=ev,win=wl,side=side,n=n,gross=v.mean(),net=v.mean()-COST,t=t,t1=t1,t2=t2,wy=int((yr>0).sum()),ny=len(yr)))
R=pd.DataFrame(rows); R.to_csv('_bt_gold_opens_0919.csv',index=False)
ok=R[(R.t>3)&(R.t1>1.5)&(R.t2>1.5)&(R.wy>=R.ny*0.8)&(R.net>0)]
def show(df):
    for _,r in df.iterrows(): print(f"  {r.ev:16s} {r.win:8s} {r.side} n{int(r.n)} グロス{r.gross:+.4f}% ネット{r.net:+.4f}% t{r.t:+.2f}(前{r.t1:+.1f}/後{r.t2:+.1f}) 勝ち年{int(r.wy)}/{int(r.ny)}")
print(f'総セル{len(R)}(市場イベント{len(EV)}×窓{len(WIN)}×売買) 判定=t>3×前後半t>1.5×勝ち年8割×ネット>0(コスト{COST}%)')
print(f'\n== 合格 {len(ok)} ==');show(ok.sort_values('t',ascending=False))
print('\n== 参考: t上位12(合格不問) ==');show(R.sort_values('t',ascending=False).head(12))
print('\n== 本人が挙げた市場(東京/韓国)の全窓 ==');show(R[R.ev.str.contains('東京寄り|韓国寄り')].sort_values(['ev','win']))
