"""GER40 01→16JST を実装可能な定義(火〜金JST・直前レッグ≤0)でポートフォリオに足す。EURJPY換算。"""
import pandas as pd, numpy as np
JPY=148.0
D=pd.read_pickle('_xm_multi_hist.pkl'); H=D['h1']; S=D['spec']; sp=S['GER40Cash']; spread_pct=sp['spread']/sp['bid']*100
def jst(df):
    df=df.copy(); df.index=pd.to_datetime(df['time'],unit='s'); df=df[df.index>='2016-05-01']; out=[]
    for ts in df.index:
        y=ts.year; mar=max(d for d in range(25,32) if pd.Timestamp(y,3,d).dayofweek==6); octb=max(d for d in range(25,32) if pd.Timestamp(y,10,d).dayofweek==6)
        dst=pd.Timestamp(y,3,mar,1)<=ts<pd.Timestamp(y,10,octb,1); out.append(ts-pd.Timedelta(hours=3 if dst else 2)+pd.Timedelta(hours=9))
    df.index=pd.DatetimeIndex(out); return df
g=jst(H['GER40Cash']); ej=jst(H['EURUSD']).close.resample('1D').last().ffill()*jst(H['USDJPY']).close.resample('1D').last().ffill()
o=g.open; a=o[o.index.hour==1]; b=o[o.index.hour==16]; res=[]
for ts,pa in a.items():
    nb=b[(b.index>ts)&(b.index<=ts+pd.Timedelta(hours=26))]
    if len(nb): res.append((ts.normalize(),pa,nb.iloc[0],(nb.index[0]-ts).total_seconds()/3600))
L=pd.DataFrame(res,columns=['d','a','b','hrs']).set_index('d'); L['pct']=(L.b/L.a-1)*100; L['net']=L.pct-spread_pct-sp['swap_long']/365*(L.hrs/24)
F=L[L.pct.shift(1)<=0]                          # 火〜金JST・直前レッグ≤0 (n=935)
y=F.net.groupby(F.index.year).sum(); print('採用定義: n=%d net/回%+.3f%% t=%+.2f 勝ち年%d/%d 年率%+.1f%% 最悪%+.1f%%'%(len(F),F.net.mean(),F.net.mean()/F.net.std()*np.sqrt(len(F)),(y>0).sum(),len(y),F.net.sum()/10.3,F.net.min()))
print('年別net%:',y.round(1).to_dict())
gj=(F.a*F.net/100*0.1)*ej.reindex(F.index).ffill()
gold=pd.read_pickle('_bt_gold_fix_trades.pkl')['net']; N=pd.read_pickle('_xm_night_legs.pkl')
def filt(X): X=X[X.pct.shift(1)<=0]; return X[X.index.dayofweek<4]
jp=filt(N['JP225Cash']); us=filt(N['US500Cash'])
X=pd.DataFrame({'g':gold,'j':jp.a*jp.net/100,'u':us.a*us.net/100*JPY*0.1,'d':gj}).fillna(0); X=X[X.index>='2016-05-26']
print('GER40 0.1lot 1晩sd=%.0f円 名目(直近)=%.0f万円 相関 j-d %.2f u-d %.2f'%(X.d[X.d!=0].std(),25553*0.1*ej.iloc[-1]/1e4,X[(X.j!=0)&(X.d!=0)][['j','d']].corr().iloc[0,1],X[(X.u!=0)&(X.d!=0)][['u','d']].corr().iloc[0,1]))
def run(P,xd,B0=58966,stop=25000):
    B=B0;peak=B;dd=0;st=False;mn=B; G=P.g.values;J=P.j.values;U=P.u.values;Dd=P.d.values
    for i in range(len(G)):
        if B<stop: st=True
        if not st:
            pnl=np.floor(B/50000)/100*100*G[i]*JPY+np.floor(B/7000*10)/10*J[i]+np.floor(B/40000)*U[i]+(np.floor(B/xd)*Dd[i] if xd>0 else 0)
            B=max(0,B+pnl)
        peak=max(peak,B); dd=min(dd,B/peak-1); mn=min(mn,B)
    return B,dd,st
rng=np.random.default_rng(5); yrs=sorted(set(X.index.year)); bl={y:X[X.index.year==y] for y in yrs}; SS=[rng.choice(yrs,5) for _ in range(300)]
print('\n== 現行(金/日経7000/US500 40000)+GER40(火〜金・直前≤0) X円/0.1lot: 5年 ==')
for xd in [0,150000,100000,80000,60000,50000,40000]:
    e=[];st=0;dds=[]
    for s in SS:
        P=pd.concat([bl[y] for y in s]); B,dd,x=run(P,xd); e.append(B); st+=x; dds.append(dd)
    e=np.array(e); print(f'  GER40 {xd:>7}円/0.1lot: 中央{np.median(e)/1e4:6.1f} 下位10%{np.percentile(e,10)/1e4:5.1f} 上位10%{np.percentile(e,90)/1e4:6.1f} 停止{st/3:3.0f}% DD中央{np.median(dds)*100:.0f}% E[log]{np.mean(np.log(np.maximum(e,1)/58966)):+.2f}')
print('\n実パス 2016-05→ (5.9万起点):')
for xd in [0,80000,60000,50000]:
    B,dd,st=run(X,xd); print(f'  GER40 {xd:>6}: 終値{B/1e4:.1f}万 DD{dd*100:.0f}% {"停止" if st else ""}')
print('1年ごと(GER40 60000追加/なし):')
for yv in yrs:
    P=bl[yv]; b1,_,_=run(P,60000); b0,_,_=run(P,0); print(f'  {yv}: なし{b0/1e4:5.1f}万 → あり{b1/1e4:5.1f}万')
F.to_pickle('_ger40_night_legs.pkl')
