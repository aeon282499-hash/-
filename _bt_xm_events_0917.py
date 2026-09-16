"""イベント夜(未検証): JP SQ(第2金曜)の前夜・US月次オプション/クワドラプル(第3金曜)の前夜・祝日前夜(JP=1321の営業日から検出・US=連邦祝日+聖金曜日)を現行レッグで分ける"""
import pandas as pd, numpy as np
src=open('_bt_xm_recheck_0917.py',encoding='utf-8').read().split("print('\n== A")[0]; exec(src)
for k,v in {'JP225Cash':3.5-1.8,'US500Cash':8.2-1.3,'GER40Cash':7.9}.items(): S[k]['swap_long']=v
LJ=legs('JP225Cash',15,9,[0,1,2,3],80); LU=legs('US500Cash',15,9,[0,1,2,3],80); LG=legs('GER40Cash',1,16,[1,2,3,4],26)
j=LJ[(LJ.prev<=0)|(LJ.dow==0)]; u=LU[(LU.prev<=0)|(LU.dow==0)]; g=LG[LG.prev<=0]
def nth_friday(y,m,n):
    d=pd.Timestamp(y,m,1); d=d+pd.Timedelta(days=(4-d.dayofweek)%7); return d+pd.Timedelta(days=7*(n-1))
yrs=range(2016,2027)
sq=set(nth_friday(y,m,2) for y in yrs for m in range(1,13)); msq=set(nth_friday(y,m,2) for y in yrs for m in [3,6,9,12])
opx=set(nth_friday(y,m,3) for y in yrs for m in range(1,13)); qw=set(nth_friday(y,m,3) for y in yrs for m in [3,6,9,12])
def split(L,days,lab):
    # 木曜JST建て(dow=3)の夜が金曜イベントの前夜
    thu=L[L.dow==3]; ev=thu[[(d+pd.Timedelta(days=1)) in days for d in thu.index]]; rest=L.drop(ev.index)
    print(f'  {lab:28s} 該当: {stats(ev.net)} | それ以外: {stats(rest.net)}')
print('== 日経 (前夜≤0 or 月曜) ==')
split(j,sq,'SQ前夜(第2金曜の木曜)'); split(j,msq,'メジャーSQ前夜'); split(j,opx,'米OPEX前夜'); split(j,qw,'米クワドラプル前夜')
print('== US500 ==')
split(u,opx,'OPEX前夜(第3金曜の木曜)'); split(u,qw,'クワドラプル前夜'); split(u,sq,'日本SQ前夜')
print('== GER40 (01→16JST・欧州の夜) ==')
# GER40の金曜JST玉(dow=4)=欧州木曜の夜→金曜16:00JST決済。第3金曜=欧州のオプション満期(Eurex)
fri=g[g.dow==4]; ev=fri[[d.normalize() in opx for d in fri.index]]; print('  第3金曜JST(Eurex満期日)の玉    :',stats(ev.net),'| 他の金曜:',stats(fri.drop(ev.index).net))
# 祝日前夜
etf=pd.read_csv('_etf1321_20y.csv'); dcol=[c for c in etf.columns if 'date' in c.lower() or '日' in c][0]; jdays=set(pd.to_datetime(etf[dcol]).dt.normalize())
def next_bday_missing(d,days,horizon=1):
    n=d+pd.Timedelta(days=1)
    while n.dayofweek>=5: n+=pd.Timedelta(days=1)
    return n not in days
jj=j.copy(); jj['prehol']=[next_bday_missing(d,jdays) for d in jj.index]
print('== 祝日前夜 ==')
print('  日経 翌営業日が東証休場   :',stats(jj.net[jj.prehol]),'| 通常:',stats(jj.net[~jj.prehol]))
# 東証休場日そのものの夜(当日9-15時に現物なし)
jj['hol_today']=[d.normalize() not in jdays for d in jj.index]
print('  日経 当日が東証休場(CFDのみ):',stats(jj.net[jj.hol_today]),'| 通常:',stats(jj.net[~jj.hol_today]))
from pandas.tseries.holiday import USFederalHolidayCalendar
from dateutil.easter import easter
us=set(USFederalHolidayCalendar().holidays('2016-01-01','2026-12-31'))
us|=set(pd.Timestamp(easter(y))-pd.Timedelta(days=2) for y in yrs)   # Good Friday
us-= set(d for d in us if d.month in (10,11) and d.day<15 and d.dayofweek==0)  # Columbus(NYSEは開場)
us-= set(d for d in us if d.month==11 and d.day==11)                            # Veterans
# US500の夜(15→9JST)が跨ぐ米セッションは同日。翌米営業日が祝日=祝日前夜
uu=u.copy(); uu['prehol']=[next_bday_missing(d,set(pd.bdate_range('2016','2027'))-us) for d in uu.index]
uu['hol_today']=[d.normalize() in us for d in uu.index]
print('  US500 翌米営業日が祝日      :',stats(uu.net[uu.prehol]),'| 通常:',stats(uu.net[~uu.prehol]))
print('  US500 当日が米祝日(現物なし) :',stats(uu.net[uu.hol_today]),'| 通常:',stats(uu.net[~uu.hol_today]))
# 全夜(フィルタ無し)でも祝日前夜を見る
LJ2=LJ.copy(); LJ2['prehol']=[next_bday_missing(d,jdays) for d in LJ2.index]; print('  日経 全夜での祝日前夜       :',stats(LJ2.net[LJ2.prehol]),'| 通常:',stats(LJ2.net[~LJ2.prehol]))
LU2=LU.copy(); LU2['prehol']=[next_bday_missing(d,set(pd.bdate_range('2016','2027'))-us) for d in LU2.index]; print('  US500 全夜での祝日前夜      :',stats(LU2.net[LU2.prehol]),'| 通常:',stats(LU2.net[~LU2.prehol]))
