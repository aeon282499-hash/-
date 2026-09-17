"""2026-09-17 株CFD(XM)の日次イベント型: ギャップフェード/ギャップ反発/急落翌日反発/急騰翌日ショート/3日急落反発/日中の条件付き夜。
   コスト=各バーのspread列(points×point/価格)+スワップ(mode5=年%・US-8.65%・1泊×1.4)。日足は取引所セッション(H1のサーバー日付)から作る。"""
import pandas as pd, numpy as np, pickle, sys, warnings; warnings.filterwarnings('ignore')
src=sys.argv[1] if len(sys.argv)>1 else '_xm_stocks_h1.pkl'
d=pickle.load(open(src,'rb')); print('銘柄',len(d),src)
ev=[]
for s,v in d.items():
    h=v['h1'].copy(); sp=v['spec']
    if len(h)<2000: continue
    h['t']=pd.to_datetime(h['time'],unit='s'); h['date']=h.t.dt.normalize(); h=h[h.t.dt.dayofweek<5]
    pt=sp.get('point') or 0.01
    g=h.groupby('date').agg(o=('open','first'),c=('close','last'),hi=('high','max'),lo=('low','min'),spo=('spread','first'),spc=('spread','last'),n=('close','size'))
    g=g[(g.n>=3)&(g.o>0)&(g.c>0)]
    if len(g)<500: continue
    reg=sp['path'].split(chr(92))[1]
    swap=(sp['swap_long'] if sp.get('swap_mode')==5 else -8.65)/365*1.4   # %/泊(負=コスト)
    g['sp_o']=g.spo*pt/g.o*100; g['sp_c']=g.spc*pt/g.c*100   # 往復スプレッド%
    g['pc']=g.c.shift(1); g['gap']=(g.o/g.pc-1)*100; g['day']=(g.c/g.o-1)*100; g['ret']=(g.c/g.pc-1)*100
    g['no']=g.o.shift(-1); g['nc']=g.c.shift(-1); g['night']=(g.no/g.c-1)*100; g['nday']=(g.nc/g.c-1)*100
    g['ret3']=(g.c/g.c.shift(3)-1)*100
    sp_med=g.sp_o.replace(0,np.nan).median()
    if not (sp_med>0): continue
    g['sp_o']=g.sp_o.replace(0,sp_med); g['sp_c']=g.sp_c.replace(0,sp_med)
    g['sym']=s; g['reg']=reg; g['swap']=swap; ev.append(g)
E=pd.concat(ev); E=E[E.index>='2012-01-01']; print('日数',len(E),'銘柄',E.sym.nunique(),'地域',E.reg.value_counts().to_dict(), 'スプレッド中央%',E.sp_o.median().round(3))
def st(x,lab):
    x=x.dropna(); 
    if len(x)<30: print(f'  {lab:44s} n={len(x)} (少)'); return
    y=x.groupby(x.index.year).sum(); n=len(x); t=x.mean()/x.std()*np.sqrt(n); h=x.index<pd.Timestamp('2019-07-01'); a=x[h]; b=x[~h]
    ta=a.mean()/a.std()*np.sqrt(len(a)) if len(a)>10 else np.nan; tb=b.mean()/b.std()*np.sqrt(len(b)) if len(b)>10 else np.nan
    x3=x.drop(x.nlargest(3).index); t3=x3.mean()/x3.std()*np.sqrt(len(x3))
    print(f'  {lab:44s} n={n:6d} 回{x.mean():+.3f}% t={t:+.2f} 前{a.mean():+.3f}(t{ta:+.1f}) 後{b.mean():+.3f}(t{tb:+.1f}) 勝{int((y>0).sum())}/{len(y)} 上3除t{t3:+.2f}')
for reg in ['US','EU']:
    D=E[E.reg==reg]
    if len(D)<1000: continue
    print(f'\n===== {reg} ({D.sym.nunique()}銘柄・{len(D)}日) =====')
    print('-- ギャップアップ寄り売り→引け買戻し(日中・スワップ無し) --')
    for th in [2,3,5,8]: m=D.gap>=th; st(-(D.day[m])-D.sp_o[m],f'GU≥{th}% 寄り空売り→引け')
    print('-- ギャップダウン寄り買い→引け --')
    for th in [2,3,5,8]: m=D.gap<=-th; st(D.day[m]-D.sp_o[m],f'GD≤-{th}% 寄り買い→引け')
    print('-- 急落日の引け買い→翌寄り(1泊)/翌引け --')
    for th in [3,5,8,12]:
        m=D.ret<=-th; st(D.night[m]-D.sp_c[m]+D.swap[m],f'当日≤-{th}% 引け買い→翌寄り'); st(D.nday[m]-D.sp_c[m]+D.swap[m],f'当日≤-{th}% 引け買い→翌引け')
    print('-- 急騰日の引け売り→翌寄り --')
    for th in [3,5,8,12]: m=D.ret>=th; st(-D.night[m]-D.sp_c[m]+D.swap[m]*0.16,f'当日≥+{th}% 引け空売り→翌寄り')
    print('-- 3日累計急落の引け買い→翌引け --')
    for th in [8,12,20]: m=D.ret3<=-th; st(D.nday[m]-D.sp_c[m]+D.swap[m],f'3日≤-{th}% 引け買い→翌引け')
    print('-- 夜(引け→翌寄り)の無条件/日中条件付き --')
    st(D.night-D.sp_c+D.swap,'全夜 買い'); 
    for th in [1,2,4]: m=D.day<=-th; st(D.night[m]-D.sp_c[m]+D.swap[m],f'日中≤-{th}%の夜 買い')
    for th in [1,2,4]: m=D.day>=th; st(-D.night[m]-D.sp_c[m],f'日中≥+{th}%の夜 売り')
    print('-- 日中(寄り→引け)の無条件 --'); st(D.day-D.sp_o,'全日 寄り買い→引け'); st(-D.day-D.sp_o,'全日 寄り売り→引け')
