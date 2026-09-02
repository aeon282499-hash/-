# -*- coding: utf-8 -*-
"""_bt_buy_regime_gate.py — 買いの「反発レジーム」を自己参照で検出できるか（2026-09-03）。
指標 = 現行入口条件の全候補について「翌々日終値/寄り-1（2日目リターン）」の日次平均を、
直近126営業日で平均したもの（3営業日ラグ＝データが判明してから使う）。>0 なら反発レジームとみなす。
地合いゲート(指数)は3連敗済み。これは指数ではなくルール自身の挙動を見る。26年で判定。
"""
import pickle, json, numpy as np, pandas as pd
P = pickle.load(open("_bt_buy_20y_wide.pkl","rb")); C = P["C"]
C["score"] = (1/(1+((C.rsi-38)/8)**2)*0.30 + 1/(1+((C.dev+3)/2)**2)*0.30 + np.log10(np.maximum(C.tov,1)/1e9+1)/3*0.40)
order = np.lexsort((-C.score.to_numpy(), C.entry.to_numpy())); C = C.iloc[order].reset_index(drop=True)
OP,HI,LO,CL,RS = (P[k][order] for k in ("OP","HI","LO","CL","RS")); n=len(C); E=C.E.to_numpy()
cur = (C.rsi<=45)&(C.dev<=-1.5)&((C.rr>=1.5)|(C.vr>=2.0))&(C.tov>=2e9)&(C.atr<=3.0)
ok_base = (cur & (C.price<=10000) & ~C.nofill).to_numpy()
days = sorted(C.entry.unique()); gdi={d:i for i,d in enumerate(days)}; DAY=C.entry.map(gdi).to_numpy()
# 指標: 現行条件候補(NOFILL問わず)の2日目リターン日次平均 → 直近126日平均を3日ラグで
d2 = (CL[:,1]-E)/E*100
tmp = pd.DataFrame({"d":DAY[cur.to_numpy()], "r":d2[cur.to_numpy()]}).groupby("d").r.mean()
series = pd.Series(np.nan, index=range(len(days))); series[tmp.index]=tmp.values
ind = series.rolling(126, min_periods=60).mean().shift(3)
IND = ind.reindex(DAY).to_numpy()
def replay(tp,stop,hold,rsith):
    valid=np.isfinite(E)&np.isfinite(CL[:,hold-1]); pnl=np.full(n,np.nan); exo=np.zeros(n,dtype=np.int8); done=~valid
    sl=E*(1-stop/100); tl=E*(1+tp/100)
    for k in range(hold):
        live=~done
        if k>0:
            op=OP[:,k]; g=live&np.isfinite(op)&(op>0)&((op<=sl)|(op>=tl)); pnl[g]=(op[g]-E[g])/E[g]*100; exo[g]=k; done|=g; live=~done
        s=live&(LO[:,k]<=sl); pnl[s]=-stop; exo[s]=k; done|=s; live=~done
        t=live&(HI[:,k]>=tl); pnl[t]=tp; exo[t]=k; done|=t; live=~done
        rc=(RS[:,k]>=rsith)&np.isfinite(RS[:,k]); r=live&(rc|(k==hold-1)); pnl[r]=(CL[r,k]-E[r])/E[r]*100; exo[r]=k; done|=r
    return pnl,exo
pnl,exo=replay(5,3,3,50)
SECMAP=json.load(open("sector33_map.json",encoding="utf-8")); TICK=C.ticker.to_numpy(); YEAR=C.year.to_numpy()
SEC=np.array([SECMAP.get(t) or f"__u{t}" for t in TICK],dtype=object)
by_day=[[] for _ in days]
for i,d in enumerate(DAY): by_day[d].append(i)
def run(ok):
    ok=ok&np.isfinite(pnl); ou={}; os_={}; picks=[]
    for d in range(len(days)):
        for tk in [t for t,u in ou.items() if u<d]: del ou[tk]; del os_[tk]
        sc={}
        for s in os_.values(): sc[s]=sc.get(s,0)+1
        cnt=0
        for i in by_day[d]:
            if cnt>=5: break
            if not ok[i] or TICK[i] in ou: continue
            s=SEC[i]
            if sc.get(s,0)>=3: continue
            cnt+=1; ex=min(d+int(exo[i]),len(days)-1); ou[TICK[i]]=ex; os_[TICK[i]]=s; sc[s]=sc.get(s,0)+1
            picks.append((d,ex,YEAR[i],pnl[i],E[i],IND[i]))
    live=[]; rows=[]
    for d,ex,y,p,e,ii in picks:
        live=[x for x in live if x>=d]
        if len(live)>=3: continue
        sh=int(1e6/e/100)*100
        if sh<=0: continue
        live.append(ex); rows.append({"y":y,"yen":p/100*sh*e,"pnl":p,"ind":ii})
    return pd.DataFrame(rows)
def pf(x):
    g=x[x>0].sum(); l=-x[x<=0].sum(); return g/l if l else np.inf
R=run(ok_base)
print(f"現行(全部撃つ): n={len(R)} 合計{R.yen.sum():+,.0f}")
print("\n指標の水準別（エントリー時点の直近126日・2日目リターン平均）:")
R["b"]=pd.cut(R.ind,[-9,-0.5,-0.25,0,0.25,0.5,9],labels=["<-0.5","-0.5〜-0.25","-0.25〜0","0〜0.25","0.25〜0.5",">0.5"])
g=R.groupby("b",observed=True); print(pd.DataFrame({"n":g.size(),"avg%":g.pnl.mean().round(3),"PF":g.yen.apply(pf).round(2),"合計":g.yen.sum().round(0)}).to_string())
for th in (-0.25, 0.0, 0.25):
    Rg=run(ok_base & (IND>th)); Rs=run(ok_base & ~(IND>th))
    print(f"\n[ゲート 指標>{th:+.2f} で撃つ]  n={len(Rg)} 合計{Rg.yen.sum():+,.0f}  / 止めていた側 n={len(Rs)} 合計{Rs.yen.sum():+,.0f}")
    for lo,hi,nm in ((2001,2008,"01-08"),(2009,2016,"09-16"),(2017,2021,"17-21"),(2022,2026,"22-26")):
        a=Rg[(Rg.y>=lo)&(Rg.y<=hi)]; b=R[(R.y>=lo)&(R.y<=hi)]
        print(f"   {nm}: ゲート後 n={len(a):>4} PF={pf(a.yen):.2f} {a.yen.sum():>+12,.0f} | 現行 n={len(b):>4} PF={pf(b.yen):.2f} {b.yen.sum():>+12,.0f}")
    yy=Rg.groupby("y").yen.sum().reindex(range(2001,2027),fill_value=0); print("   年別(万):", {int(k):round(v/1e4) for k,v in yy.items()})
print("\n指標の年別平均（>0が反発レジーム）:", {int(k):round(v,2) for k,v in pd.Series(IND).groupby(YEAR).mean().items()})
