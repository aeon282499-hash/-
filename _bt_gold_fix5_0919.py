"""9/19 本人「5分ショートでもいい」→ LBMA AM/PM値決め直前 5/10/15分ショートを実弾と同じ土台(SL$10・コスト$0.21/oz・ゲートN40>0.10・E[log]ポートフォリオ)で比較。XM M5実データ17ヶ月で裏取り。シドニー寄りの人工物判定。"""
import pandas as pd, numpy as np, io, contextlib
COST=0.21; SL=10.0
m1=pd.read_pickle('_fx_xauusd_m1.pkl'); lon=m1.index.tz_localize('UTC').tz_convert('Europe/London'); df=m1.copy(); df['ld']=lon.date; df['ltm']=lon.time
def trades(h0,m0,h1,m1_):
    w=df[(df.ltm>=pd.Timestamp(f'{h0:02d}:{m0:02d}').time())&(df.ltm<pd.Timestamp(f'{h1:02d}:{m1_:02d}').time())]; g=w.groupby('ld')
    t=pd.DataFrame({'entry':g['open'].first(),'exit':g['close'].last(),'hi':g['high'].max(),'n':g.size()}); t=t[t.n>=(h1*60+m1_-h0*60-m0)*0.6]
    t.index=pd.to_datetime(t.index); t=t[(t.index.dayofweek<5)&(t.entry!=t.exit)]
    t['gross']=t.entry-t.exit; t.loc[t.hi>=t.entry+SL,'gross']=-SL; t['net']=t.gross-COST; return t
W={'AM 15分(現行)':(10,15,10,30),'AM 10分':(10,20,10,30),'AM 5分':(10,25,10,30),'PM 15分':(14,45,15,0),'PM 10分':(14,50,15,0),'PM 5分':(14,55,15,0)}
T={k:trades(*v) for k,v in W.items()}
print('== ① 10年Dukascopy・実弾と同じ計算(SL$10・コスト$0.21/oz固定) ==')
print(f'{"窓":12s} {"n":>5s} {"net$/oz":>8s} {"$/回":>6s} {"勝率":>4s} {"PF":>5s} {"t":>5s} {"前/後":>11s} 勝年  年別net$/oz')
for k,t in T.items():
    s=t.net; n=len(s); m=n//2; yr=s.groupby(s.index.year).sum(); pf=s[s>0].sum()/-s[s<0].sum()
    print(f'{k:12s} {n:5d} {s.sum():+8.1f} {s.mean():+6.3f} {(s>0).mean()*100:3.0f}% {pf:5.2f} {s.mean()/s.std()*np.sqrt(n):+5.2f} {s[:m].mean()/s[:m].std()*np.sqrt(m):+5.1f}/{s[m:].mean()/s[m:].std()*np.sqrt(n-m):+5.1f} {int((yr>0).sum()):2d}/{len(yr)}  '+' '.join(f'{y%100:02d}:{v:+.0f}' for y,v in yr.items()))
print('\n   ゲート(直近40回平均>0.10$)込み:')
G={}
for k,t in T.items():
    s=t.net; gate=(s.rolling(40).mean().shift(1)>0.10).fillna(False); g=s.where(gate,0.0); G[k]=g; x=s[gate]; yr=g.groupby(g.index.year).sum()
    print(f'   {k:12s} 稼働{gate.mean()*100:3.0f}% n{len(x):4d} $/回{x.mean():+.3f} 合計{x.sum():+7.1f} 勝年{int((yr>0).sum())}/{len(yr)}  '+' '.join(f'{y%100:02d}:{v:+.0f}' for y,v in yr.items()))
# ポートフォリオE[log]: 稼働4本の土台で金列を差し替え
buf=io.StringIO()
with contextlib.redirect_stdout(buf):
    src=open('_bt_xm_grid_0917.py',encoding='utf-8').read().split("print(f'== コスト=")[0]; exec(src)
X0=Xm(2.0); BASE=(8750,31250,50000)
def run_(P,units,xg,B0_=B0,stop=25000):
    B=B0_;st=False;peak=B;dd=0;Gg=P.g.values;Jj=P.j.values;Uu=P.u.values;Dd=P.d.values
    for i in range(len(Gg)):
        if B<stop: st=True
        if not st: B=max(0,B+np.floor(B/xg)/100*100*Gg[i]*JPY+np.floor(B/units[0]*10)/10*Jj[i]+np.floor(B/units[1])*Uu[i]+np.floor(B/units[2])*Dd[i])
        peak=max(peak,B); dd=min(dd,B/peak-1)
    return B,st,dd
def ev_(X,xg,seed=5):
    rng=np.random.default_rng(seed); yrs=sorted(set(X.index.year)); bl={y:X[X.index.year==y] for y in yrs}; e=[];st=0;dds=[]
    for _ in range(300):
        P=pd.concat([bl[y] for y in rng.choice(yrs,5)]); B,x,dd=run_(P,BASE,xg); e.append(B);st+=x;dds.append(dd)
    e=np.array(e); e1=[]
    for _ in range(400):
        P=bl[rng.choice(yrs)]; B,x,dd=run_(P,BASE,xg); e1.append(B)
    return f'E[log]{np.mean(np.log(np.maximum(e,1)/B0)):+.2f} 5年中央{np.median(e)/1e4:7.1f}万 下位10%{np.percentile(e,10)/1e4:5.1f} 停止{st/3:3.0f}% DD{np.median(dds)*100:4.0f}% | 1年中央{np.median(e1)/1e4:5.1f} 下位10%{np.percentile(e1,10)/1e4:4.1f}'
print('\n== ② ポートフォリオ(日経/US500/GER40は現行・金列を差し替え・金2.5万円/0.01・全停止2.5万) ==')
def withg(g):
    X=X0.copy(); X['g']=g.reindex(X.index).fillna(0.0); return X
for lab,g in [('現行 AM15分',G['AM 15分(現行)']),('AM10分',G['AM 10分']),('AM5分',G['AM 5分']),('PM5分だけ',G['PM 5分']),('AM15分+PM5分(各ゲート)',G['AM 15分(現行)'].add(G['PM 5分'],fill_value=0)),('AM15分+PM15分',G['AM 15分(現行)'].add(G['PM 15分'],fill_value=0)),('AM5分+PM5分',G['AM 5分'].add(G['PM 5分'],fill_value=0))]:
    print(f'  {lab:24s}',ev_(withg(g),25000))
print('  金サイズ1.25万での AM15分+PM5分:',ev_(withg(G['AM 15分(現行)'].add(G['PM 5分'],fill_value=0)),12500))
# ③ XM M5実データで裏取り
print('\n== ③ XM口座M5(サーバー時刻-2h=ロンドン) 同窓の裏取り vs 同期間Dukascopy ==')
m5=pd.read_pickle('_xm_m5_recent.pkl')['GOLD.'].copy(); m5['t']=pd.to_datetime(m5.time,unit='s')-pd.Timedelta(hours=2); m5=m5.set_index('t'); m5=m5[m5.index.dayofweek<5]
d0=m5.index[0].normalize()
for k,(h0,m0,h1,mm) in W.items():
    w=m5[(m5.index.hour*60+m5.index.minute>=h0*60+m0)&(m5.index.hour*60+m5.index.minute<h1*60+mm)]; g=w.groupby(w.index.date)
    x=g.open.first()-g.close.last(); x=x[g.size()>=(h1*60+mm-h0*60-m0)//5]; n=len(x)
    dk=T[k].gross[T[k].index>=d0]
    print(f'  {k:12s} XM: n{n} グロス{x.mean():+.3f}$/oz t{x.mean()/x.std()*np.sqrt(n):+.2f} 勝率{(x>0).mean()*100:.0f}% | Duka同期間: n{len(dk)} グロス{dk.mean():+.3f} t{dk.mean()/dk.std()*np.sqrt(len(dk)):+.2f}')
print('\n== ④ シドニー10:00買い(0→15分)をXM M15 4年でDST分割(AEDT=10〜4月はUTC23:00=XM再開直後) ==')
D=pd.read_pickle('_xm_metals_m15_0919.pkl')['GOLD.']; D=D[D.index.dayofweek<5]
syd=D.index.tz_localize('Europe/London',ambiguous='NaT',nonexistent='NaT').tz_convert('Australia/Sydney')
r=(D.open.shift(-1)/D.open-1)*100; mask=(syd.hour==10)&(syd.minute==0)
for lab,mk in [('AEDT(夏・UTC23:00)',mask&(syd.strftime('%Z')=='AEDT')),('AEST(冬・UTC00:00)',mask&(syd.strftime('%Z')=='AEST'))]:
    x=r[mk.fillna(False)].dropna(); print(f'  {lab:18s} n{len(x)} 平均{x.mean():+.4f}% t{x.mean()/x.std()*np.sqrt(len(x)):+.2f}')
