"""2026-09-17 本人「トレード件数が不満」→ 現行ルールの年間件数と、件数を増やす案(フィルタ外し/ゲート外し/GER40火曜無条件)を修正済みコスト(div)で測る。"""
import pandas as pd, numpy as np, sys
src=open('_bt_xm_recheck_0917.py',encoding='utf-8').read().split("print('\n== A")[0]; exec(src)
sw={'JP225Cash':3.5-1.8,'US500Cash':8.2-1.3,'GER40Cash':7.9}
for k,v in sw.items(): S[k]['swap_long']=v
LJ=legs('JP225Cash',15,9,[0,1,2,3],80); LU=legs('US500Cash',15,9,[0,1,2,3],80); LG=legs('GER40Cash',1,16,[1,2,3,4],26)
oj=jst(H['JP225Cash']).open; spj=S['JP225Cash']['spread']/S['JP225Cash']['bid']*100
def jp_series(jsel,mult=2.0):
    j=jsel.copy(); m1=[]
    for t in j.index:
        x=oj[(oj.index>t+pd.Timedelta(hours=15))&(oj.index<t+pd.Timedelta(hours=33))&(oj.index.hour==1)]; m1.append(x.iloc[0] if len(x) else np.nan)
    j['m1']=m1; base=j.a*j.net/100; mid=(j.m1/j.a-1)*100; rest=(j.b/j.m1-1)*100-spj
    return base+(j.m1*rest/100*mult).where(mid<=-0.5,0), (mid<=-0.5)
gate=(gold.rolling(40).mean().shift(1)>0.10).fillna(False); gg=gold.where(gate,0.0)
def X_of(jsel,usel,gsel,gate_on=True,mult=2.0):
    js,addm=jp_series(jsel,mult); gs=gg if gate_on else gold
    X=pd.DataFrame({'g':gs,'j':js,'u':usel.a*usel.net/100*0.1*uj.reindex(usel.index).ffill(),'d':gsel.a*gsel.net/100*0.1*ej.reindex(gsel.index).ffill()}).fillna(0); X=X[X.index>='2016-05-26']
    nyr=len(set(X.index.year))-1  # 2016は半年
    cnt=dict(gold=int((gs!=0).sum()) if gate_on else int(gold.notna().sum()),jp=len(jsel[jsel.index>='2016-05-26']),jpadd=int(addm[addm.index>='2016-05-26'].sum()),us=len(usel[usel.index>='2016-05-26']),ger=len(gsel[gsel.index>='2016-05-26']))
    return X,cnt,nyr
def ev(X,units=(8750,31250,50000),xg=25000,seed=5):
    rng=np.random.default_rng(seed); yrs=sorted(set(X.index.year)); bl={y:X[X.index.year==y] for y in yrs}; e=[];st=0;dds=[]
    for _ in range(300):
        P=pd.concat([bl[y] for y in rng.choice(yrs,5)]); B,x,dd=run(P,units,xg); e.append(B); st+=x; dds.append(dd)
    e=np.array(e); e1=[]
    for _ in range(400):
        P=bl[rng.choice(yrs)]; B,x,dd=run(P,units,xg); e1.append(B)
    e1=np.array(e1)
    return f"E[log]{np.mean(np.log(np.maximum(e,1)/B0)):+.2f} 5年中央{np.median(e)/1e4:6.1f}万 下位10%{np.percentile(e,10)/1e4:5.1f} 停止{st/3:3.0f}% DD{np.median(dds)*100:4.0f}% | 1年中央{np.median(e1)/1e4:5.1f} 下位10%{np.percentile(e1,10)/1e4:4.1f}"
def yen_year(X):  # 現行枚数(6.9枚/0.1/0.1/金0.02)固定での年平均円
    s=(X.g*0.02*100*JPY+X.j*6.9+X.u*1+X.d*1); y=s.groupby(s.index.year).sum(); return y[y.index>=2017].mean()/1e4
cur_j=LJ[(LJ.prev<=0)|(LJ.dow==0)]; cur_u=LU[(LU.prev<=0)|(LU.dow==0)]; cur_g=LG[LG.prev<=0]
V=[('現行(前夜≤0・月曜無条件・GER直前≤0・金ゲート)',cur_j,cur_u,cur_g,True),
   ('金ゲート外す(毎日撃つ)',cur_j,cur_u,cur_g,False),
   ('US500を無条件(月〜木毎晩)',cur_j,LU,cur_g,True),
   ('GER40を無条件(火〜金毎晩)',cur_j,cur_u,LG,True),
   ('GER40の火曜JSTだけ無条件',cur_j,cur_u,LG[(LG.prev<=0)|(LG.dow==1)],True),
   ('日経を無条件(月〜木毎晩)',LJ,cur_u,cur_g,True),
   ('指数3本とも無条件',LJ,LU,LG,True),
   ('全部外す(指数無条件+金毎日)',LJ,LU,LG,False)]
print('== 年間件数と成績(修正済みコスト・現行単位 8750/31250/50000・追加×2.0・金2.5万/0.01) ==')
for lab,js,us,gs,gt in V:
    X,c,n=X_of(js,us,gs,gt); tot=sum(c.values())
    print(f"\n[{lab}]\n  件数/年: 金{c['gold']/n:4.0f} 日経{c['jp']/n:4.0f}(+追加{c['jpadd']/n:3.0f}) US500{c['us']/n:4.0f} GER40{c['ger']/n:4.0f} = 合計{tot/n:4.0f}件/年 ≈ 週{tot/n/52:.1f}件")
    print(f"  {ev(X)}  | 今の枚数固定なら年{yen_year(X):+.1f}万円")
print('\n== レッグ別: フィルタ内 vs フィルタ外の玉(修正済みコスト) ==')
for lab,L,m in [('日経',LJ,(LJ.prev<=0)|(LJ.dow==0)),('US500',LU,(LU.prev<=0)|(LU.dow==0)),('GER40',LG,LG.prev<=0)]:
    print(f'  {lab} 内: {stats(L.net[m])}\n  {lab} 外: {stats(L.net[~m])}')
print(f'  金 ゲート内: n={int(gate.sum())} 平均{gold[gate].mean():+.3f}$/oz  ゲート外: n={int((~gate).sum())} 平均{gold[~gate].mean():+.3f}$/oz')
