"""9/19 本人「徐々にロット増やして・フル全自動」→ 残高しきい値で単位を÷k する自動ラダーのBS(修正コスト・金ゲート込み・全停止2.5万)。"""
import pandas as pd, numpy as np
src=open('_bt_xm_grid_0917.py',encoding='utf-8').read().split("print(f'== コスト=")[0]; exec(src)
X=Xm(2.0); BASE=(8750,31250,50000); XG=25000
def kof(B,lad):
    k=1.0
    for th,kk in lad:
        if B>=th: k=kk
    return k
def run_l(P,lad,B0_=B0,stop=25000):
    B=B0_;st=False;peak=B;dd=0;kmax=1.0; Gg=P.g.values;Jj=P.j.values;Uu=P.u.values;Dd=P.d.values
    for i in range(len(Gg)):
        if B<stop: st=True
        if not st:
            k=kof(B,lad); kmax=max(kmax,k); u=(BASE[0]/k,BASE[1]/k,BASE[2]/k); xg=XG/k
            B=max(0,B+np.floor(B/xg)/100*100*Gg[i]*JPY+np.floor(B/u[0]*10)/10*Jj[i]+np.floor(B/u[1])*Uu[i]+np.floor(B/u[2])*Dd[i])
        peak=max(peak,B); dd=min(dd,B/peak-1)
    return B,st,dd,kmax
def evl(lad,B0_=B0,seed=5):
    rng=np.random.default_rng(seed); yrs=sorted(set(X.index.year)); bl={y:X[X.index.year==y] for y in yrs}
    e=[];st=0;dds=[];km=[]
    for _ in range(300):
        P=pd.concat([bl[y] for y in rng.choice(yrs,5)]); B,x,dd,k=run_l(P,lad,B0_); e.append(B);st+=x;dds.append(dd);km.append(k)
    e=np.array(e); e1=[];s1=0;d1=[]
    for _ in range(400):
        P=bl[rng.choice(yrs)]; B,x,dd,k=run_l(P,lad,B0_); e1.append(B);s1+=x;d1.append(dd)
    e1=np.array(e1)
    return dict(elog=np.mean(np.log(np.maximum(e,1)/B0_)),med=np.median(e)/1e4,p10=np.percentile(e,10)/1e4,stop=st/3,dd=np.median(dds)*100,ddw=np.percentile(dds,10)*100,
                med_1y=np.median(e1)/1e4,p10_1y=np.percentile(e1,10)/1e4,stop1=s1/4,dd1=np.median(d1)*100,kmax=np.median(km))
def f(r): return f"E[log]{r['elog']:+.2f} 5年中央{r['med']:7.1f}万 下位10%{r['p10']:5.1f} 停止{r['stop']:3.0f}% DD中央{r['dd']:4.0f}%/悪10%{r['ddw']:4.0f}% | 1年中央{r['med_1y']:5.1f} 下位10%{r['p10_1y']:4.1f} 停止{r['stop1']:3.0f}% DD{r['dd1']:4.0f}% | k到達{r['kmax']:.2f}"
L={
 'A 現行×1固定':[],
 'B ≥10万で×1.25':[(1e5,1.25)],
 'C ≥10万×1.25 ≥30万×1.5':[(1e5,1.25),(3e5,1.5)],
 'D ≥10万×1.25 ≥30万×1.5 ≥100万×2':[(1e5,1.25),(3e5,1.5),(1e6,2.0)],
 'E ≥15万×1.25 ≥50万×1.5':[(1.5e5,1.25),(5e5,1.5)],
 'F ≥20万×1.25 ≥50万×1.5 ≥150万×2':[(2e5,1.25),(5e5,1.5),(1.5e6,2.0)],
 'G 今すぐ×1.25 ≥30万×1.5':[(0,1.25),(3e5,1.5)],
 'H ≥10万×1.25 ≥30万×1.5 ≥100万×2 ≥300万×3':[(1e5,1.25),(3e5,1.5),(1e6,2.0),(3e6,3.0)],
}
print(f'B0={B0}')
for k,v in L.items(): print(f'{k:38s}',f(evl(v)))
print('\n== 残高12万スタート(入金後) ==')
for k in ['A 現行×1固定','B ≥10万で×1.25','C ≥10万×1.25 ≥30万×1.5','D ≥10万×1.25 ≥30万×1.5 ≥100万×2']: print(f'{k:38s}',f(evl(L[k],120000)))
