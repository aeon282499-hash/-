import MetaTrader5 as mt5, pandas as pd
ok=mt5.initialize(); print('init',ok, mt5.last_error())
ai=mt5.account_info(); print('account',ai.login if ai else None, ai.server if ai else None, ai.balance if ai else None)
S=mt5.symbols_get(); print('total symbols',len(S))
rows=[(s.name,s.path,s.trade_mode,s.spread,s.bid,s.swap_long,s.swap_short,s.swap_mode,s.volume_min,s.trade_contract_size,s.currency_profit) for s in S]
df=pd.DataFrame(rows,columns=['name','path','tmode','spread','bid','swl','sws','swm','vmin','contract','cur'])
df['grp']=df.path.str.split('\\').str[0]
print(df.groupby('grp').size().to_string())
cr=df[df.path.str.contains('rypto|BTC|ETH',case=False)|df.name.str.contains('BTC|ETH|LTC|XRP',case=False)]
pd.set_option('display.width',250); print(cr.to_string(index=False))
df.to_csv('_xm_symbols_all_0917.csv',index=False)
mt5.shutdown()
