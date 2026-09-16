"""未取得の全銘柄(FXクロス/エキゾチック/金属バリエーション/先物)のH1をMT5から取得 → _xm_rest_h1.pkl"""
import MetaTrader5 as mt5, pandas as pd, pickle, os
have=set(pd.read_pickle('_xm_multi_hist.pkl')['h1'].keys())
for f in ['_xm_more_idx_h1.pkl','_xm_metals_fx2_h1.pkl']: have|=set(pickle.load(open(f,'rb')).keys())
print('既存',len(have),sorted(have))
mt5.initialize(); out={}
for s in mt5.symbols_get():
    if s.name in have or s.path.startswith('Stocks'): continue
    mt5.symbol_select(s.name,True); i=mt5.symbol_info(s.name)
    r=mt5.copy_rates_from_pos(s.name,mt5.TIMEFRAME_H1,0,99000)
    if r is None or len(r)<1500: print('  skip',s.name,0 if r is None else len(r)); continue
    df=pd.DataFrame(r); bid=i.bid if i.bid>0 else float(df.close.iloc[-1]); spread=i.spread*i.point if i.spread>0 else float((df.spread.replace(0,pd.NA).dropna().median() if 'spread' in df else 0)*i.point)
    out[s.name]={'h1':df,'spec':dict(path=s.path,swap_long=i.swap_long,swap_short=i.swap_short,swap_mode=i.swap_mode,cur=i.currency_profit,vmin=i.volume_min,contract=i.trade_contract_size,bid=bid,point=i.point,spread=spread)}
    print(f'  {s.name:14s} {len(df):6d}本 {pd.to_datetime(df.time.iloc[0],unit="s").date()}〜 spread{spread/bid*100:.4f}% swL{i.swap_long} swS{i.swap_short} mode{i.swap_mode}')
mt5.shutdown(); pickle.dump(out,open('_xm_rest_h1.pkl','wb')); print('取得',len(out))
