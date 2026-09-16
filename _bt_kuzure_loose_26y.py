# 「貸借○ × 本日崩れ(5MA割れ初日・急騰+15%・出来高1.3x・代金5億) × 当日-5%以下」を26年で測る
import io,sys,pickle; sys.stdout=io.TextIOWrapper(sys.stdout.buffer,encoding='utf-8')
import numpy as np, pandas as pd
C=pickle.load(open("_bt_crashshort_26y_pool.pkl","rb"))
ISS=pickle.load(open("_iss_type_by_year.pkl","rb")); yrs=sorted(ISS)
def iss(row):
    y=min(yrs,key=lambda k:abs(k-row.year)); return ISS[y].get(row.ticker[:4],"?")
C["iss"]=C.apply(iss,axis=1)
def era(y): return "01-08" if y<=2008 else "09-16" if y<=2016 else "17-21" if y<=2021 else "22-26"
def pf(x): x=np.asarray(x,float); n=-x[x<=0].sum(); return x[x>0].sum()/n if n else 99
def slot(D,k=3,size=300_000):
    D=D.sort_values(["entry","vr"],ascending=[True,False]); rows=[]
    for d,g in D.groupby("entry"):
        for r in g.head(k).itertuples():
            sh=int(size/r.O1/100)*100
            if sh<=0: continue
            rows.append((r.year,r.pnl,r.pnl/100*sh*r.O1))
    return pd.DataFrame(rows,columns=["y","pnl","yen"])
def show(lab,D,k=3):
    if len(D)==0: print(f"  {lab:<34} 0件"); return
    R=slot(D,k); yy=R.groupby("y").yen.sum()
    e={n:D[D.year.map(era)==n] for n in ("01-08","09-16","17-21","22-26")}
    print(f"  {lab:<34} n{len(D):>5}({len(D)/26:>3.0f}/年) {D.pnl.mean():>+5.2f}% 勝率{(D.pnl>0).mean()*100:>4.1f}% PF{pf(D.pnl):>4.2f} | "+" ".join(f"{k_}:{x.pnl.mean():+.2f}/PF{pf(x.pnl):.2f}(n{len(x)})" for k_,x in e.items())+f" | {k}件×30万 26年{R.yen.sum()/1e4:>+5,.0f}万 勝年{int((yy>0).sum())}/{len(yy)} 最悪年{yy.min()/1e4:+,.0f}")
loose=(C.run20>=15)&(C.chg<=-5)&(C.vr>=1.3)&(C.tov>=5e8)
print("■ 26年・貸借判定は2017以降の最近傍年(2016以前は2017の区分を流用＝目安)")
show("緩い型 全銘柄",C[loose])
show("緩い型 貸借○",C[loose&(C.iss=="2")])
show("緩い型 貸借✕(信用)",C[loose&(C.iss=="1")])
show("緩い型 貸借○ GD-3%以下見送り",C[loose&(C.iss=="2")&(C.gap>-3)])
print("■ 貸借判定が実在する2017-26だけ")
M=C.year>=2017
show("緩い型 全銘柄 17-26",C[loose&M])
show("緩い型 貸借○ 17-26",C[loose&M&(C.iss=="2")])
show("緩い型 貸借✕ 17-26",C[loose&M&(C.iss=="1")])
show("緩い型 貸借○ GD-3%見送り 17-26",C[loose&M&(C.iss=="2")&(C.gap>-3)])
show("💥本番(急騰30/2x) 貸借○ 17-26",C[M&(C.iss=="2")&(C.run20>=30)&(C.vr>=2)&(C.chg<=-5)])
print("■ 面(貸借○・17-26): 急騰 × 出来高")
for r20 in (15,20,30):
    for vv in (1.3,1.5,2.0):
        show(f"急騰≥{r20} 出来高≥{vv}x 当日≤-5",C[M&(C.iss=="2")&(C.run20>=r20)&(C.vr>=vv)&(C.chg<=-5)])
print("■ 当日下落の深さ(貸借○・17-26・急騰15/1.3x)")
for lo,hi in ((-5,-3),(-8,-5),(-12,-8),(-99,-12)):
    show(f"当日{lo}〜{hi}%",C[M&(C.iss=="2")&(C.run20>=15)&(C.vr>=1.3)&(C.chg>lo)&(C.chg<=hi)])
print("■ 年別(緩い型 貸借○ 3件×30万)")
R=slot(C[loose&(C.iss=="2")]); yy=R.groupby("y").yen.sum()/1e4; print("  "+" ".join(f"{y%100:02d}:{v:+.0f}" for y,v in yy.items()))
