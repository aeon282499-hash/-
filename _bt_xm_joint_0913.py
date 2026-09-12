"""(1)日経×US500×GER40 の残高単位を同時に振る(金は0.01固定) (2)クロス市場フィルタ: 日経夜をUS500/GER40の直前レッグで、GER40をJP/US500の直前で条件付け"""
import pandas as pd, numpy as np, itertools
D=pd.read_pickle('_xm_multi_hist.pkl'); H=D['h1']
def jst(df):
    df=df.copy(); df.index=pd.to_datetime(df['time'],unit='s'); df=df[df.index>='2016-05-01']; out=[]
    for ts in df.index:
        y=ts.year; mar=max(d for d in range(25,32) if pd.Timestamp(y,3,d).dayofweek==6); octb=max(d for d in range(25,32) if pd.Timestamp(y,10,d).dayofweek==6)
        dst=pd.Timestamp(y,3,mar,1)<=ts<pd.Timestamp(y,10,octb,1); out.append(ts-pd.Timedelta(hours=3 if dst else 2)+pd.Timedelta(hours=9))
    df.index=pd.DatetimeIndex(out); return df
uj=jst(H['USDJPY']).close.resample('1D').last().ffill(); ej=jst(H['EURUSD']).close.resample('1D').last().ffill()*uj
gold=pd.read_pickle('_bt_gold_fix_trades.pkl')['net']; N=pd.read_pickle('_xm_night_legs.pkl'); JPY=148.0
jpA=N['JP225Cash']; usA=N['US500Cash']; deA=pd.read_pickle('_ger40_night_legs.pkl')   # deA はフィルタ済み
def filt(X): X=X[X.pct.shift(1)<=0]; return X[X.index.dayofweek<4]
jp=filt(jpA); us=filt(usA)
X=pd.DataFrame({'g':gold,'j':jp.a*jp.net/100,'u':us.a*us.net/100*0.1*uj.reindex(us.index).ffill(),'d':deA.a*deA.net/100*0.1*ej.reindex(deA.index).ffill()}).fillna(0); X=X[X.index>='2016-05-26']
def run(P,xj,xu,xd,B0=58966,stop=25000):
    B=B0;peak=B;dd=0;st=False; G=P.g.values;J=P.j.values;U=P.u.values;Dd=P.d.values
    for i in range(len(G)):
        if B<stop: st=True
        if not st:
            pnl=np.floor(B/50000)/100*100*G[i]*JPY+np.floor(B/xj*10)/10*J[i]+np.floor(B/xu)*U[i]+np.floor(B/xd)*Dd[i]
            B=max(0,B+pnl)
        peak=max(peak,B); dd=min(dd,B/peak-1)
    return B,dd,st
rng=np.random.default_rng(5); yrs=sorted(set(X.index.year)); bl={y:X[X.index.year==y] for y in yrs}; SS=[rng.choice(yrs,5) for _ in range(300)]
rows=[]
for xj,xu,xd in itertools.product([5000,7000,10000],[25000,40000,80000],[40000,50000,80000]):
    e=[];st=0;dds=[]
    for s in SS:
        P=pd.concat([bl[y] for y in s]); B,dd,x=run(P,xj,xu,xd); e.append(B); st+=x; dds.append(dd)
    e=np.array(e); rows.append(dict(xj=xj,xu=xu,xd=xd,med=np.median(e)/1e4,p10=np.percentile(e,10)/1e4,stop=st/3,dd=np.median(dds)*100,elog=np.mean(np.log(np.maximum(e,1)/58966))))
R=pd.DataFrame(rows).sort_values('elog',ascending=False); pd.set_option('display.width',200)
print('== 同時グリッド(27通り) E[log]順 ==');print(R.round(2).to_string(index=False))
print('現行(7000/40000/50000):'); print(R[(R.xj==7000)&(R.xu==40000)&(R.xd==50000)].round(2).to_string(index=False))
# (2) クロスフィルタ
def stats(n):
    y=n.groupby(n.index.year).sum(); return f'n={len(n):4d} net/回{n.mean():+.3f}% t={n.mean()/n.std()*np.sqrt(len(n)):+.2f} 勝{int((y>0).sum())}/{len(y)} 前{n[n.index.year<=2020].mean():+.3f} 後{n[n.index.year>2020].mean():+.3f}'
print('\n== クロス市場フィルタ ==')
J=jpA.copy(); J=J[J.index.dayofweek<4]; J['own']=jpA.pct.shift(1).reindex(J.index)
usprev=usA.pct.shift(0)   # US500の直前レッグ = 前日15:00→当日9:00 (当日9:00に確定) → 当日15:00の日経建てで既知
J['us_prev']=usA.pct.shift(1).reindex(J.index)  # 前日建てのUS500レッグ(当日9時に確定)
J['us_prev']=usA.pct.reindex(J.index-pd.Timedelta(days=1)).values if False else usA.pct.shift(1).reindex(J.index)
print('日経(前夜≤0)          :',stats(J[J.own<=0].net))
print('日経(前夜≤0 & US前夜≤0):',stats(J[(J.own<=0)&(J.us_prev<=0)].net)); print('日経(前夜≤0 & US前夜>0):',stats(J[(J.own<=0)&(J.us_prev>0)].net))
print('日経(前夜>0 & US前夜≤0):',stats(J[(J.own>0)&(J.us_prev<=0)].net))
# GER40 を 同日の日経の夜(前日15:00→当日9:00・当日01:00時点では未確定)は使えない → 前日の日経夜(2日前15:00→前日9:00)で
Dg=deA.copy(); Dg['jp_prev']=jpA.pct.shift(1).reindex(Dg.index-pd.Timedelta(days=0)).values
Dg['jp_prev']=jpA.pct.reindex(Dg.index-pd.Timedelta(days=1)).values     # 前日15:00→当日9:00 は当日01:00時点で未確定なので使わない。2日前→前日 を使用
Dg['jp_prev2']=jpA.pct.reindex(Dg.index-pd.Timedelta(days=2)).values
print('GER40(直前≤0)              :',stats(Dg.net))
print('GER40(直前≤0 & 日経前々夜≤0):',stats(Dg[Dg.jp_prev2<=0].net)); print('GER40(直前≤0 & 日経前々夜>0):',stats(Dg[Dg.jp_prev2>0].net))
Dg['us_prev']=usA.pct.reindex(Dg.index-pd.Timedelta(days=1)).values   # 前日15:00→当日9:00 未確定 → 前々日→前日
print('GER40(直前≤0 & US500前日レッグ≤0):',stats(Dg[Dg.us_prev<=0].net)); print('GER40(直前≤0 & US500前日レッグ>0):',stats(Dg[Dg.us_prev>0].net))
