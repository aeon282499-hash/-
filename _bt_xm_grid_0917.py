"""スワップ符号バグ修正+実測コスト(9/14-16約定)+配当で、レッグ単位(枚数)・追加倍率・金サイズを再グリッド。従来BTは spec の swap_long が負値で格納されているのを引いていた(=スワップを収入として計上)。"""
import pandas as pd, numpy as np, itertools, sys
src=open('_bt_xm_recheck_0917.py',encoding='utf-8').read().split("print('\n== A")[0]; exec(src)
MODE=sys.argv[1] if len(sys.argv)>1 else 'div'
sw={'div':{'JP225Cash':3.5-1.8,'US500Cash':8.2-1.3,'GER40Cash':7.9},'meas':{'JP225Cash':3.5,'US500Cash':8.2,'GER40Cash':7.9}}[MODE]
for k,v in sw.items(): S[k]['swap_long']=v      # 正値=コスト(legs()は引く)
LJ=legs('JP225Cash',15,9,[0,1,2,3],80); LU=legs('US500Cash',15,9,[0,1,2,3],80); LG=legs('GER40Cash',1,16,[1,2,3,4],26)
j=LJ[(LJ.prev<=0)|(LJ.dow==0)].copy(); u=LU[(LU.prev<=0)|(LU.dow==0)]; g=LG[LG.prev<=0]
oj=jst(H['JP225Cash']).open; spj=S['JP225Cash']['spread']/S['JP225Cash']['bid']*100
m1=[]
for t in j.index:
    x=oj[(oj.index>t+pd.Timedelta(hours=15))&(oj.index<t+pd.Timedelta(hours=33))&(oj.index.hour==1)]; m1.append(x.iloc[0] if len(x) else np.nan)
j['m1']=m1; base=j.a*j.net/100; mid=(j.m1/j.a-1)*100; rest=(j.b/j.m1-1)*100-spj
gate=(gold.rolling(40).mean().shift(1)>0.10).fillna(False); gg=gold.where(gate,0.0)
def Xm(mult):
    ad=base+(j.m1*rest/100*mult).where(mid<=-0.5,0)
    X=pd.DataFrame({'g':gg,'j':ad,'u':u.a*u.net/100*0.1*uj.reindex(u.index).ffill(),'d':g.a*g.net/100*0.1*ej.reindex(g.index).ffill()}).fillna(0); return X[X.index>='2016-05-26']
def ev(X,units,xg,seed=5):
    rng=np.random.default_rng(seed); yrs=sorted(set(X.index.year)); bl={y:X[X.index.year==y] for y in yrs}; e=[];st=0;dds=[]
    for _ in range(300):
        P=pd.concat([bl[y] for y in rng.choice(yrs,5)]); B,x,dd=run(P,units,xg); e.append(B); st+=x; dds.append(dd)
    e=np.array(e); e1=[]
    for _ in range(400):
        P=bl[rng.choice(yrs)]; B,x,dd=run(P,units,xg); e1.append(B)
    e1=np.array(e1)
    return dict(elog=np.mean(np.log(np.maximum(e,1)/B0)),med=np.median(e)/1e4,p10=np.percentile(e,10)/1e4,stop=st/3,dd=np.median(dds)*100,p10_1y=np.percentile(e1,10)/1e4,med_1y=np.median(e1)/1e4)
def fmt(r): return f"E[log]{r['elog']:+.2f} 中央{r['med']:6.1f}万 下位10%{r['p10']:5.1f} 停止{r['stop']:3.0f}% DD{r['dd']:4.0f}% | 1年中央{r['med_1y']:5.1f} 下位10%{r['p10_1y']:4.1f}"
print(f'== コスト={MODE} 現行設定(8750/31250/50000・追加×1.0・金5万ゲート込み) ==')
X1=Xm(1.0); print('  現行:',fmt(ev(X1,(8750,31250,50000),50000)))
OFF=1e12
print('  レッグ寄与(現行から1本抜く):')
for lab,units in [('US500抜き',(8750,OFF,50000)),('GER40抜き',(8750,31250,OFF)),('日経抜き',(OFF,31250,50000))]: print(f'    {lab:10s}',fmt(ev(X1,units,50000)))
print('    金抜き    ',fmt(ev(X1,(8750,31250,50000),0)))
print('\n== グリッド: 日経{7000,8750,10000,12500}×US{25000,31250,40000,off}×GER{40000,50000,80000,off}×追加倍率{1,1.5,2}・金2.5万ゲート込み ==')
rows=[]
Xs={m:Xm(m) for m in [1.0,1.5,2.0]}
for jpu,usu,deu,m in itertools.product([7000,8750,10000,12500],[25000,31250,40000,OFF],[40000,50000,80000,OFF],[1.0,1.5,2.0]):
    r=ev(Xs[m],(jpu,usu,deu),25000); r.update(jp=jpu,us=usu,de=deu,mult=m); rows.append(r)
R=pd.DataFrame(rows); R.to_csv(f'_bt_xm_grid_0917_{MODE}.csv',index=False)
R2=R.sort_values('elog',ascending=False)
print('  E[log]上位12:')
for _,r in R2.head(12).iterrows(): print(f"    JP{r.jp:6.0f} US{('off' if r.us>1e9 else f'{r.us:.0f}'):>6s} GER{('off' if r.de>1e9 else f'{r.de:.0f}'):>6s} ×{r['mult']:.1f}: {fmt(r)}")
print('  停止0%かつDD≤-53%以内で E[log]上位8:')
R3=R[(R.stop==0)&(R.dd>=-53)].sort_values('elog',ascending=False)
for _,r in R3.head(8).iterrows(): print(f"    JP{r.jp:6.0f} US{('off' if r.us>1e9 else f'{r.us:.0f}'):>6s} GER{('off' if r.de>1e9 else f'{r.de:.0f}'):>6s} ×{r['mult']:.1f}: {fmt(r)}")
print('  現行単位(8750/31250/50000)での倍率×金サイズ:')
for m in [1.0,1.5,2.0]:
    for xg in [50000,25000]: print(f'    ×{m:.1f} 金{xg/1e4:.1f}万:',fmt(ev(Xs[m],(8750,31250,50000),xg)))
