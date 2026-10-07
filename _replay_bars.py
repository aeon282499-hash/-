# -*- coding: utf-8 -*-
"""_replay_bars.py — 再生でサインが出た銘柄の5分足を、ラインを出した足の前後10本で表示（確認用・10/8）"""
import sys, json
from datetime import datetime, timedelta
import numpy as np, pandas as pd
import fibo_oct as O
from fibo_live import BarBuilder
from fibo_daytrade import hm, Bar
sys.stdout.reconfigure(encoding='utf-8')
def bars5(day, code):
    out=[]
    for d in (O.MINUTES_DIR/f'{day}.jsonl',):
        rows,_=O.read_rows(d); b=BarBuilder(5)
        for row in rows:
            ts=datetime.strptime(row['ts'],'%Y-%m-%d %H:%M:%S'); t=hm(ts)
            if t<'09:00' or '11:30'<t<'12:30' or t>'15:30': continue
            if t in ('11:30','15:30'): ts=ts.replace(second=0)-timedelta(seconds=1)
            v=row['s'].get(code)
            if not v: continue
            bar=b.push(ts,v[0],v[2],v[3],v[4])
            if bar: out.append(bar)
        if b.cur is not None: c=b.cur; out.append(Bar(b.cur_key,c['o'],c['h'],c['l'],c['c'],0.0))
    return out
def prev_day(day):
    d=datetime.strptime(day,'%Y-%m-%d').date()-timedelta(days=1)
    while not (O.MINUTES_DIR/f'{d.isoformat()}.jsonl').exists() or O.is_stale_day(O.MINUTES_DIR/f'{d.isoformat()}.jsonl'): d-=timedelta(days=1)
    return d.isoformat()
for day, code, name, armed in [l.split() for l in sys.argv[1:]]:
    pb=bars5(prev_day(day),code); tb=bars5(day,code); allb=pb[-40:]+tb; off=len(allb)-len(tb)
    df=pd.DataFrame([dict(時刻=hm(b.t),始値=b.o,高値=b.h,安値=b.l,終値=b.c) for b in allb])
    df['25MA']=df.終値.rolling(25).mean().round(1); df['直近12本高値']=df.高値.rolling(12).max()
    df['幅%']=((df.高値/df.安値-1)*100).round(2); df['押し%']=((1-df.高値/df.直近12本高値)*100).round(2); df['陰線']=np.where(df.終値<df.始値,'●','')
    j=df.index[(df.index>=off)&(df.時刻==armed)]
    if not len(j): print(f'\n■ {name}({code}) {day} ライン足{armed} が見つからない'); continue
    j=j[0]; w=df.loc[max(0,j-10):min(len(df)-1,j+10)].copy(); w.insert(0,'',np.where(w.index==j,'◀ライン','')); w.insert(0,' ',np.where(w.index<off,'前日',''))
    print(f'\n■ {name}({code}) {day} ラインを出した足={armed}（この足が陰線・安値>25MA・幅<0.8%・押し≥0.8%・25MA上向き）'); print(w.to_string(index=False))
