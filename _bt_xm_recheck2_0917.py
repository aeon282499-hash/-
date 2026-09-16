"""追試2: 日経追加(倍率/2段目)とゲート込み金サイズの頑健性 = 最悪夜・上位3日除去・年別・1年地平"""
import pandas as pd, numpy as np
src=open('_bt_xm_recheck_0917.py',encoding='utf-8').read().split("print('\n== A")[0]; exec(src)
oj=jst(H['JP225Cash']).open; spj=S['JP225Cash']['spread']/S['JP225Cash']['bid']*100
def mid_at(o,L,h):
    m=[]
    for t in L.index:
        x=o[(o.index>t+pd.Timedelta(hours=15))&(o.index<t+pd.Timedelta(hours=33))&(o.index.hour==h)]; m.append(x.iloc[0] if len(x) else np.nan)
    return pd.Series(m,index=L.index)
m3=mid_at(oj,j,3); mid3=(m3/j.a-1)*100; rest3=(j.b/m3-1)*100-spj
LOT=6.7
def nightly(ad): return (ad*LOT)  # 円/夜 at 6.7lot固定
print('== 日経 夜別損益(6.7枚固定・円): 最悪5夜と上位3夜除去後の合計 ==')
variants={'追加なし':base,'01時≤-0.5%×1.0(現行)':add,'×1.5':base+(j.m1*rest/100*1.5).where(mid<=-0.5,0),'×2.0':base+(j.m1*rest/100*2.0).where(mid<=-0.5,0),
 '現行+03時≤-0.75%2段目':add+(m3*rest3/100).where(mid3<=-0.75,0),'現行+03時≤-1.0%2段目':add+(m3*rest3/100).where(mid3<=-1.0,0)}
for lab,ad in variants.items():
    n=nightly(ad).dropna(); w=n.nsmallest(5); top=n.nlargest(3)
    y=n.groupby(n.index.year).sum()
    print(f'  {lab:24s} 合計{n.sum()/1e4:+6.1f}万 上位3夜除去{(n.sum()-top.sum())/1e4:+6.1f}万 最悪夜{w.iloc[0]/1e4:+.1f}万({w.index[0].date()}) 最悪5夜平均{w.mean()/1e4:+.1f}万 勝ち年{int((y>0).sum())}/{len(y)} 年別:'+' '.join(f'{k}:{v/1e4:+.1f}' for k,v in y.items()))
print('  (残高5.9万に対し最悪夜の比率で見る)')
# 追加玉の独立検定: 追加が発動した夜だけの残り区間、上位3日除去、前後半
m=(mid<=-0.5); r=rest[m]
print(f'\n追加玉(01時≤-0.5%)残り区間: {stats(r)}  上位3日除去: {stats(r.drop(r.nlargest(3).index))}')
m2=(mid3<=-0.75); r2=rest3[m2]
print(f'2段目玉(03時≤-0.75%)残り区間: {stats(r2)}  上位3日除去: {stats(r2.drop(r2.nlargest(3).index))}  01時追加と重複{((mid<=-0.5)&m2).sum()}/{m2.sum()}夜')
# 1年地平のBS
def eval1(X,units=(8750,31250,50000),xg=50000,seed=7):
    rng=np.random.default_rng(seed); yrs=sorted(set(X.index.year)); bl={y:X[X.index.year==y] for y in yrs}; e=[]
    for _ in range(600):
        P=bl[rng.choice(yrs)]; B,x,dd=run(P,units,xg); e.append(B)
    e=np.array(e); return f'1年: 中央{np.median(e)/1e4:5.1f}万 下位10%{np.percentile(e,10)/1e4:4.1f} 減る{(e<B0).mean()*100:3.0f}%'
print('\n== 1年地平 ==')
for lab,ad in variants.items(): print(f'  {lab:24s}',eval1(Xof(ad)))
print('\n== 金ゲート込みサイズの頑健性 ==')
gate=(gold.rolling(40).mean().shift(1)>0.10).fillna(False); gg=gold.where(gate,0.0)
top=gg.nlargest(3); print(f'  ゲート込み合計{gg.sum():+.0f}$/oz 上位3日{top.values.round(1)} 除去後{gg.sum()-top.sum():+.0f}  最悪日{gg.min():+.1f}$/oz  日数{int(gate.sum())}')
for xg,lab in [(50000,'今 5万円/0.01'),(25000,'2.5万円/0.01'),(12500,'1.25万円/0.01')]:
    print(f'  {lab:14s}',eval1(Xof(add,gser=gg),xg=xg),' | 上位3日除去:',evalp(Xof(add,gser=gg.drop(top.index)),xg=xg))
# ゲートのパラメータ違いで2.5万が保つか
for N,thr in [(20,0.1),(60,0.1),(40,0.0),(40,0.2),(120,0.1)]:
    gt=(gold.rolling(N).mean().shift(1)>thr).fillna(False); g2=gold.where(gt,0.0)
    print(f'  ゲートN{N}/閾{thr}: 5万 {evalp(Xof(add,gser=g2),xg=50000)} | 2.5万 {evalp(Xof(add,gser=g2),xg=25000)}')
