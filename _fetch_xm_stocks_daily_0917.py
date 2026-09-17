"""株CFD全銘柄: H1を取得→その場で日足(取引所セッション=サーバー日付)に集約して保存(メモリ節約)。既存 _xm_stocks_all_h1.pkl(300銘柄)は変換して引き継ぐ。"""
import MetaTrader5 as mt5, pandas as pd, pickle, os, gc
OUT='_xm_stocks_all_daily.pkl'
def to_daily(h):
    h=h.copy(); h['t']=pd.to_datetime(h['time'],unit='s'); h=h[h.t.dt.dayofweek<5]; h['date']=h.t.dt.normalize()
    return h.groupby('date').agg(o=('open','first'),c=('close','last'),hi=('high','max'),lo=('low','min'),spo=('spread','first'),spc=('spread','last'),n=('close','size'))
out=pickle.load(open(OUT,'rb')) if os.path.exists(OUT) else {}
if os.path.exists('_xm_stocks_all_h1.pkl') and len(out)<300:
    old=pickle.load(open('_xm_stocks_all_h1.pkl','rb'))
    for k,v in old.items(): out[k]=dict(d=to_daily(v['h1']),spec=v['spec'])
    del old; gc.collect(); pickle.dump(out,open(OUT,'wb')); print('引き継ぎ',len(out),flush=True)
mt5.initialize(); syms=[s for s in mt5.symbols_get() if s.path.startswith('Stocks') and s.name not in out]; print('残り',len(syms),flush=True); fail=[]
for n,s in enumerate(syms):
    mt5.symbol_select(s.name,True); i=mt5.symbol_info(s.name); r=mt5.copy_rates_from_pos(s.name,mt5.TIMEFRAME_H1,0,99000)
    if r is None or len(r)<200: fail.append(s.name); continue
    out[s.name]=dict(d=to_daily(pd.DataFrame(r)),spec=dict(path=s.path,tmode=i.trade_mode,swap_long=i.swap_long,swap_short=i.swap_short,swap_mode=i.swap_mode,cur=i.currency_profit,vmin=i.volume_min,contract=i.trade_contract_size,bid=i.bid,ask=i.ask,point=i.point,spread=i.spread,margin=i.margin_initial))
    del r
    if n%100==0: print(n,s.name,flush=True); pickle.dump(out,open(OUT,'wb'))
mt5.shutdown(); pickle.dump(out,open(OUT,'wb')); print('完了',len(out),'失敗',len(fail),fail[:20],flush=True)
