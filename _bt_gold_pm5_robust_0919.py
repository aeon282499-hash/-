"""PM値決め直前5分ショート(ロンドン14:55→15:00)の頑健性。土台は_bt_gold_fix5_0919.pyと同じ。"""
import pandas as pd, numpy as np, io, contextlib
exec(open('_bt_gold_fix5_0919.py',encoding='utf-8').read().split("print('== ① ")[0])
buf=io.StringIO()
with contextlib.redirect_stdout(buf):
    src=open('_bt_xm_grid_0917.py',encoding='utf-8').read().split("print(f'== コスト=")[0]; exec(src)
X0=Xm(2.0); BASE=(8750,31250,50000)
def run_(P,xg,B0_=B0,stop=25000):
    B=B0_;st=False;peak=B;dd=0;Gg=P.g.values;Jj=P.j.values;Uu=P.u.values;Dd=P.d.values
    for i in range(len(Gg)):
        if B<stop: st=True
        if not st: B=max(0,B+np.floor(B/xg)/100*100*Gg[i]*JPY+np.floor(B/BASE[0]*10)/10*Jj[i]+np.floor(B/BASE[1])*Uu[i]+np.floor(B/BASE[2])*Dd[i])
        peak=max(peak,B); dd=min(dd,B/peak-1)
    return B,st,dd
def ev_(g,xg=25000,seed=5):
    X=X0.copy(); X['g']=g.reindex(X.index).fillna(0.0)
    rng=np.random.default_rng(seed); yrs=sorted(set(X.index.year)); bl={y:X[X.index.year==y] for y in yrs}; e=[];st=0;dds=[]
    for _ in range(300):
        P=pd.concat([bl[y] for y in rng.choice(yrs,5)]); B,x,dd=run_(P,xg); e.append(B);st+=x;dds.append(dd)
    e=np.array(e); e1=[]
    for _ in range(400):
        P=bl[rng.choice(yrs)]; B,x,dd=run_(P,xg); e1.append(B)
    return f'E[log]{np.mean(np.log(np.maximum(e,1)/B0)):+.2f} 5年中央{np.median(e)/1e4:7.1f}万 下位10%{np.percentile(e,10)/1e4:5.1f} 停止{st/3:3.0f}% DD{np.median(dds)*100:4.0f}% | 1年中央{np.median(e1)/1e4:5.1f} 下位10%{np.percentile(e1,10)/1e4:4.1f}'
def gated(s,N=40,th=0.10): return s.where((s.rolling(N).mean().shift(1)>th).fillna(False),0.0)
am=T['AM 15分(現行)'].net; pm=T['PM 5分'].net; base=gated(am)
print('== PM5分 頑健性(ポートフォリオE[log]・現行AM15分ゲート込みに足す) ==')
print('  現行(AMのみ)               ',ev_(base))
for N in [20,40,80,120]:
    for th in [0.0,0.10,0.20]:
        print(f'  +PM5分 ゲートN{N:3d}/閾{th:.2f}     ',ev_(base.add(gated(pm,N,th),fill_value=0)))
print('  +PM5分 ゲート無し            ',ev_(base.add(pm,fill_value=0)))
top3=pm.nlargest(3).index; pm3=pm.drop(top3); print('  +PM5分 上位3日除去(N40/0.10)  ',ev_(base.add(gated(pm3),fill_value=0)),' 除去日',[str(d.date()) for d in top3])
pm2=T['PM 5分'].gross-COST*2; print('  +PM5分 コスト2倍($0.42)      ',ev_(base.add(gated(pm2),fill_value=0)))
print('  +PM5分 2016-21だけ(前半)     ',ev_(base.add(gated(pm)[pm.index<'2022'],fill_value=0)))
print('  +PM5分 2022-26だけ(後半)     ',ev_(base.add(gated(pm)[pm.index>='2022'],fill_value=0)))
# 同日相関・SL到達・%ベース
d=pd.concat([am.rename('am'),pm.rename('pm')],axis=1).dropna(); print(f'\n  同日相関 AM15分 vs PM5分: r={d.am.corr(d.pm):+.3f}  PM5分SL$10到達 {int((T["PM 5分"].gross==-SL).sum())}回/{len(pm)}  AM15分 {int((T["AM 15分(現行)"].gross==-SL).sum())}回')
g=T['PM 5分']; pct=(g.entry-g.exit)/g.entry*100; yr=pct.groupby(pct.index.year).mean()
print('  PM5分グロス%(価格非依存)年別: '+' '.join(f'{y%100:02d}:{v:+.4f}' for y,v in yr.items())+f'  全体t{pct.mean()/pct.std()*np.sqrt(len(pct)):+.2f}')
g=T['AM 15分(現行)']; pct=(g.entry-g.exit)/g.entry*100; yr=pct.groupby(pct.index.year).mean()
print('  AM15分グロス%(参考)年別:      '+' '.join(f'{y%100:02d}:{v:+.4f}' for y,v in yr.items())+f'  全体t{pct.mean()/pct.std()*np.sqrt(len(pct)):+.2f}')
print('\n== シドニー10:00買い(0→15分)をXM M15 4年でDST分割 ==')
D=pd.read_pickle('_xm_metals_m15_0919.pkl')['GOLD.']; D=D[D.index.dayofweek<5]
syd=D.index.tz_localize('Europe/London',ambiguous='NaT',nonexistent='NaT').tz_convert('Australia/Sydney')
r=pd.Series(((D.open.shift(-1)/D.open-1)*100).values,index=D.index); ok=pd.notna(syd)
h=pd.Series(np.where(ok,syd.hour,-1),index=D.index); m=pd.Series(np.where(ok,syd.minute,-1),index=D.index); z=pd.Series(np.where(ok,syd.strftime('%Z'),''),index=D.index)
for lab,zz in [('AEDT(夏・UTC23:00=XM再開5分後)','AEDT'),('AEST(冬・UTC00:00)','AEST')]:
    x=r[(h==10)&(m==0)&(z==zz)].dropna(); print(f'  {lab:28s} n{len(x)} 平均{x.mean():+.4f}% t{x.mean()/x.std()*np.sqrt(len(x)):+.2f}')
