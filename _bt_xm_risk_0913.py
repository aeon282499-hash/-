"""リスク低減の未検証3軸: ①枚数スケール(全レッグ一律) ②危機スイッチ(直近20夜の実現ボラが長期の2倍超なら枚数半分/ゼロ) ③GER40を日経の15→01時の下落で見送り"""
import pandas as pd, numpy as np
src=open('_bt_xm_addon_recheck_0913.py',encoding='utf-8').read().split("print('追加玉")[0]; exec(src)
add=base+(j.m1*rest/100).where(mid<=-0.5,0)
JPY=148.0
Xfull=X_of(add)
dJ=jst(H['JP225Cash']).open
def run2(P,scale=1.0,switch=None,stop=25000,B0=58966):
    B=B0;peak=B;dd=0;st=False;mn=B; Gg=P.g.values;Jj=P.j.values;Uu=P.u.values;Dd=P.d.values; sw=P.sw.values if switch is not None else None
    hist=[]
    for i in range(len(Gg)):
        if B<stop: st=True
        if not st:
            k=1.0
            if switch is not None and sw[i]: k=switch
            pnl=np.floor(B/50000)/100*100*Gg[i]*JPY+np.floor(B/(7000*scale)*10*k)/10*Jj[i]+np.floor(B/(25000*scale)*k)*Uu[i]+np.floor(B/(40000*scale)*k)*Dd[i]
            B=max(0,B+pnl); hist.append(pnl)
        peak=max(peak,B); dd=min(dd,B/peak-1); mn=min(mn,B)
    return B,dd,st,mn
def evalr(X,scale=1.0,switch=None,seed=5,H=5):
    rng=np.random.default_rng(seed); yrs=sorted(set(X.index.year)); bl={y:X[X.index.year==y] for y in yrs}; e=[];st=0;dds=[]
    for _ in range(300):
        P=pd.concat([bl[y] for y in rng.choice(yrs,H)]); B,dd,x,mn=run2(P,scale,switch); e.append(B); st+=x; dds.append(dd)
    e=np.array(e)
    # 1年
    e1=[]
    for _ in range(600):
        P=pd.concat([bl[y] for y in rng.choice(yrs,1)]); B,dd,x,mn=run2(P,scale,switch); e1.append(B)
    e1=np.array(e1)
    return f'5年中央{np.median(e)/1e4:6.1f} 下位10%{np.percentile(e,10)/1e4:5.1f} 停止{st/3:3.0f}% DD中央{np.median(dds)*100:4.0f}% E[log]{np.mean(np.log(np.maximum(e,1)/58966)):+.2f} | 1年: 中央{np.median(e1)/1e4:5.1f} 減る{(e1<58966).mean()*100:3.0f}% 下位10%{np.percentile(e1,10)/1e4:4.1f}'
print('== ① 枚数スケール(全レッグの残高単位×k・金は固定) ==')
for k in [1.0,1.25,1.5,2.0,3.0]:
    print(f'  ×{k:.2f} (日経{7000*k:.0f}円/枚 = 今{8.4/k:.1f}枚): {evalr(Xfull,k)}')
print('\n== ② 危機スイッチ: 3レッグ合算の日次損益(枚数固定)の直近20日sd が 全期間sdの2倍超 → 枚数半分/ゼロ ==')
fixed=(Xfull.j*8.4+Xfull.u*2+Xfull.d*1); s20=fixed.rolling(20).std().shift(1); sw=(s20>2.0*fixed.std())
X2=Xfull.copy(); X2['sw']=sw.fillna(False).values; print(f'  発動日率 {X2.sw.mean()*100:.1f}%')
print('  無し   :',evalr(X2,1.0,None)); print('  半分   :',evalr(X2,1.0,0.5)); print('  ゼロ   :',evalr(X2,1.0,0.0))
s20b=fixed.rolling(20).std().shift(1); X3=Xfull.copy(); X3['sw']=(s20b>1.5*fixed.std()).fillna(False).values; print(f'  1.5倍閾値 発動{X3.sw.mean()*100:.1f}%: 半分',evalr(X3,1.0,0.5))
print('\n== ③ GER40: 同夜の日経15:00→01:00JSTの動き(01:00で既知)で条件付け ==')
g2=LG[LG.prev<=0].copy()
jp15=dJ[dJ.index.hour==15]; jp1=dJ[dJ.index.hour==1]
mv=[]
for d in g2.index:
    a=jp15.get(d-pd.Timedelta(days=1)+pd.Timedelta(hours=15)); b=jp1.get(d+pd.Timedelta(hours=1))
    mv.append((b/a-1)*100 if (a and b) else np.nan)
g2['jpmove']=mv
for lab,m in [('日経が01時までに≤-0.5%',g2.jpmove<=-0.5),('日経が01時までに>-0.5%',g2.jpmove>-0.5),('日経≤0',g2.jpmove<=0),('日経>0',g2.jpmove>0)]:
    print(f'  {lab:22s}: {stats(g2.net[m])}')
