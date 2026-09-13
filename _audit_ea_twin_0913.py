"""EAロジックの双子(Python)とBT定義の突き合わせ 2025-01〜2026-09。
EA: JP/US 15:00JST建て(月〜木)・前夜=当日9:00始値/前日15:00始値(月曜は無条件)・GER 01:00JST建て(火〜金)・直前レッグ=直近exit足とそのentry足(26h超は一つ前へ)。
BT: 各スクリプトの定義(月曜無条件・直前レッグ≤0)。"""
import pandas as pd, numpy as np
D=pd.read_pickle('_xm_multi_hist.pkl'); H=D['h1']
def jst(df):
    df=df.copy(); df.index=pd.to_datetime(df['time'],unit='s'); df=df[df.index>='2024-11-01']; out=[]
    for ts in df.index:
        y=ts.year; mar=max(d for d in range(25,32) if pd.Timestamp(y,3,d).dayofweek==6); octb=max(d for d in range(25,32) if pd.Timestamp(y,10,d).dayofweek==6)
        dst=pd.Timestamp(y,3,mar,1)<=ts<pd.Timestamp(y,10,octb,1); out.append(ts-pd.Timedelta(hours=3 if dst else 2)+pd.Timedelta(hours=9))
    df.index=pd.DatetimeIndex(out); return df
def ea_prev_night(o,t15):        # EA PrevNightPct: 当日9:00 / 前日15:00(月曜は金曜15:00)・足が無ければ0
    day=t15.normalize(); t9=day+pd.Timedelta(hours=9); tp=day-pd.Timedelta(days=3 if day.dayofweek==0 else 1)+pd.Timedelta(hours=15)
    if t9 not in o.index or tp not in o.index: return 0.0
    return (o[t9]/o[tp]-1)*100
def ea_prev_leg(o,t1,entryH=1,exitH=16):   # EA PrevLegPct
    for back in range(0,8):
        day=t1.normalize()-pd.Timedelta(days=back); tE=day+pd.Timedelta(hours=exitH)
        if tE>t1 or tE not in o.index: continue
        tA=tE-pd.Timedelta(hours=exitH-entryH)
        if tA not in o.index: continue
        return (o[tE]/o[tA]-1)*100
    return 0.0
report=[]
for sym,tag in [('JP225Cash','日経'),('US500Cash','US500')]:
    o=jst(H[sym]).open; o=o[~o.index.duplicated()]
    days=sorted(set(o[(o.index.hour==15)&(o.index>='2025-01-01')].index.normalize()))
    ea=set(); bt=set()
    # BT定義(月曜無条件・それ以外は直前レッグ(週末レッグ含む)≤0) = _bt_xm_monday_0913 の sel()
    a=o[o.index.hour==15]; b=o[o.index.hour==9]; legs=[]
    for t,pa in a.items():
        nb=b[(b.index>t)&(b.index<=t+pd.Timedelta(hours=80))]
        if len(nb): legs.append((t.normalize(),(nb.iloc[0]/pa-1)*100,t.dayofweek))
    L=pd.DataFrame(legs,columns=['d','pct','dow']).set_index('d'); L['prev']=L.pct.shift(1)
    for d in days:
        if d.dayofweek>3: continue
        t15=d+pd.Timedelta(hours=15)
        if d.dayofweek==0 or ea_prev_night(o,t15)<=0: ea.add(d)
        if d in L.index and (d.dayofweek==0 or (L.loc[d,'prev']<=0)): bt.add(d)
    diff=sorted((ea^bt)); report.append(f'{tag}: 候補日{len([d for d in days if d.dayofweek<=3])} EA建て{len(ea)} BT建て{len(bt)} 不一致{len(diff)} {[str(x.date()) for x in diff][:8]}')
    for d in diff[:4]:
        t15=d+pd.Timedelta(hours=15); report.append(f'    {d.date()} dow{d.dayofweek} EA前夜={ea_prev_night(o,t15):+.3f} BT直前={L.loc[d,"prev"] if d in L.index else "na"}')
o=jst(H['GER40Cash']).open; o=o[~o.index.duplicated()]
days=sorted(set(o[(o.index.hour==1)&(o.index>='2025-01-01')].index.normalize()))
a=o[o.index.hour==1]; b=o[o.index.hour==16]; legs=[]
for t,pa in a.items():
    nb=b[(b.index>t)&(b.index<=t+pd.Timedelta(hours=26))]
    if len(nb): legs.append((t.normalize(),(nb.iloc[0]/pa-1)*100,t.dayofweek))
L=pd.DataFrame(legs,columns=['d','pct','dow']).set_index('d'); L['prev']=L.pct.shift(1)
ea=set(); bt=set()
for d in days:
    if d.dayofweek not in (1,2,3,4): continue
    t1=d+pd.Timedelta(hours=1)
    if ea_prev_leg(o,t1)<=0: ea.add(d)
    if d in L.index and L.loc[d,'prev']<=0: bt.add(d)
diff=sorted(ea^bt); report.append(f'GER40: 候補日{len([d for d in days if d.dayofweek in (1,2,3,4)])} EA建て{len(ea)} BT建て{len(bt)} 不一致{len(diff)} {[str(x.date()) for x in diff][:8]}')
for d in diff[:6]:
    t1=d+pd.Timedelta(hours=1); report.append(f'    {d.date()} dow{d.dayofweek} EA直前={ea_prev_leg(o,t1):+.3f} BT直前={L.loc[d,"prev"] if d in L.index else "na"}')
print('\n'.join(report))
