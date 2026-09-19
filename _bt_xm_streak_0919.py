"""9/19 本人「まだ未検証で儲かるものあるやろ」→ 唯一グレップ0件だった『エクイティ連動サイズ』(前回結果/連敗/DD中/連勝で枚数を変える)を稼働4本の土台で測る。"""
import pandas as pd, numpy as np
src=open('_bt_xm_grid_0917.py',encoding='utf-8').read().split("print(f'== コスト=")[0]; exec(src)
X=Xm(2.0); BASE=(8750,31250,50000); XG=25000
def tstat(s):
    s=np.asarray(s,float); s=s[~np.isnan(s)]
    return (s.mean()/s.std(ddof=1)*np.sqrt(len(s)) if len(s)>2 and s.std(ddof=1)>0 else np.nan), len(s), s.mean()
print('== ① レッグ別: 前回の結果で次の玉は変わるか (同レッグの直前トレード) ==')
for col,lab in [('g','金15分S($/oz)'),('j','日経夜(円/lot)'),('u','US500夜(円/0.1)'),('d','GER40夜(円/0.1)')]:
    s=X[col]; s=s[s!=0]
    pass
    prev=s.shift(1); prev2=s.shift(2)
    r=[]
    for name,m in [('前回勝ち',prev>0),('前回負け',prev<0),('2連敗後',(prev<0)&(prev2<0)),('2連勝後',(prev>0)&(prev2>0))]:
        t,n,mu=tstat(s[m]); r.append(f'{name} n{n} 平均{mu:+.1f} t{t:+.2f}')
    ta,na,mua=tstat(s); ac=s.autocorr(1)
    print(f'  {lab:16s} 全体 n{na} 平均{mua:+.1f} t{ta:+.2f} 自己相関{ac:+.3f} | '+' | '.join(r))
print('\n== ② ポートフォリオ: エクイティ連動ルールのBS (現行×1・ラダーなし・全停止2.5万) ==')
def run_r(P,rule,B0_=B0,stop=25000):
    B=B0_;st=False;peak=B;dd=0; G=P.g.values;J=P.j.values;U=P.u.values;D=P.d.values
    streak=0; hist=[]
    for i in range(len(G)):
        if B<stop: st=True
        if not st:
            k=rule(B,peak,streak,hist)
            u=(BASE[0]/k,BASE[1]/k,BASE[2]/k); xg=XG/k
            pnl=np.floor(B/xg)/100*100*G[i]*JPY+np.floor(B/u[0]*10)/10*J[i]+np.floor(B/u[1])*U[i]+np.floor(B/u[2])*D[i]
            if pnl!=0:
                streak = streak-1 if (pnl<0 and streak<=0) else (-1 if pnl<0 else (streak+1 if streak>=0 else 1))
                hist.append(pnl); hist=hist[-10:]
            B=max(0,B+pnl)
        peak=max(peak,B); dd=min(dd,B/peak-1)
    return B,st,dd
def evl(rule,B0_=B0,seed=5):
    rng=np.random.default_rng(seed); yrs=sorted(set(X.index.year)); bl={y:X[X.index.year==y] for y in yrs}
    e=[];st=0;dds=[]
    for _ in range(300):
        P=pd.concat([bl[y] for y in rng.choice(yrs,5)]); B,x,dd=run_r(P,rule,B0_); e.append(B);st+=x;dds.append(dd)
    e=np.array(e); e1=[];s1=0
    for _ in range(400):
        P=bl[rng.choice(yrs)]; B,x,dd=run_r(P,rule,B0_); e1.append(B);s1+=x
    e1=np.array(e1)
    return f"E[log]{np.mean(np.log(np.maximum(e,1)/B0_)):+.2f} 5年中央{np.median(e)/1e4:7.1f}万 下位10%{np.percentile(e,10)/1e4:5.1f} 停止{st/3:3.0f}% DD中央{np.median(dds)*100:4.0f}% | 1年中央{np.median(e1)/1e4:5.1f} 下位10%{np.percentile(e1,10)/1e4:4.1f} 停止{s1/4:3.0f}%"
R={
 'A 現行(固定×1)':            lambda B,pk,s,h:1.0,
 'B DD>-20%で×0.5':           lambda B,pk,s,h:0.5 if B/pk-1<-0.20 else 1.0,
 'C DD>-30%で×0.5':           lambda B,pk,s,h:0.5 if B/pk-1<-0.30 else 1.0,
 'D DD>-20%で×0(休む)':       lambda B,pk,s,h:1e-9 if B/pk-1<-0.20 else 1.0,
 'E 2連敗で×0.5':             lambda B,pk,s,h:0.5 if s<=-2 else 1.0,
 'F 3連敗で×0.5':             lambda B,pk,s,h:0.5 if s<=-3 else 1.0,
 'G 前回負けで×0.5':          lambda B,pk,s,h:0.5 if s<=-1 else 1.0,
 'H 前回負けで×1.5(逆張り)':  lambda B,pk,s,h:1.5 if s<=-1 else 1.0,
 'I 2連勝で×1.25':            lambda B,pk,s,h:1.25 if s>=2 else 1.0,
 'J 前回勝ちで×1.25':         lambda B,pk,s,h:1.25 if s>=1 else 1.0,
 'K DD<-10%で×1.25(逆張り)':  lambda B,pk,s,h:1.25 if B/pk-1<-0.10 else 1.0,
 'L 直近10玉合計<0で×0.5':    lambda B,pk,s,h:0.5 if (len(h)>=10 and sum(h)<0) else 1.0,
 'M 直近10玉合計<0で×1.25':   lambda B,pk,s,h:1.25 if (len(h)>=10 and sum(h)<0) else 1.0,
 'N 最高値更新中だけ×1.25':   lambda B,pk,s,h:1.25 if B>=pk else 1.0,
}
for k,v in R.items(): print(f'  {k:28s}',evl(v))
