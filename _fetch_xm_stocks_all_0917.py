"""XMの株CFD全銘柄(1,369)のH1を取得→ _xm_stocks_all_h1.pkl (path/spec付き)。既存 _xm_stocks_h1.pkl は68銘柄のみ。"""
import MetaTrader5 as mt5, pandas as pd, pickle, time, sys
mt5.initialize(); out={}; fail=[]
syms=[s for s in mt5.symbols_get() if s.path.startswith('Stocks')]
print('株CFD',len(syms),flush=True)
for n,s in enumerate(syms):
    mt5.symbol_select(s.name,True); i=mt5.symbol_info(s.name)
    r=mt5.copy_rates_from_pos(s.name,mt5.TIMEFRAME_H1,0,99000)
    if r is None or len(r)<200: fail.append(s.name); continue
    out[s.name]=dict(h1=pd.DataFrame(r),spec=dict(path=s.path,tmode=i.trade_mode,swap_long=i.swap_long,swap_short=i.swap_short,swap_mode=i.swap_mode,cur=i.currency_profit,vmin=i.volume_min,contract=i.trade_contract_size,bid=i.bid,ask=i.ask,point=i.point,spread=i.spread,margin=i.margin_initial))
    if n%100==0: print(n,s.name,len(r),flush=True); pickle.dump(out,open('_xm_stocks_all_h1.pkl','wb'))
mt5.shutdown(); pickle.dump(out,open('_xm_stocks_all_h1.pkl','wb')); print('取得',len(out),'失敗',len(fail),fail[:20],flush=True)
