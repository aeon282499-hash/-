import MetaTrader5 as mt5, pandas as pd, numpy as np, pickle
mt5.initialize()
allsym=mt5.symbols_get()
st=[s for s in allsym if 'tock' in s.path]
print('個別株CFD数',len(st)); print('パス例:',sorted(set(s.path.rsplit(chr(92),1)[0] for s in st))[:12])
names=['Apple','Microsoft','Amazon','Alphabet','Meta','Nvidia','Tesla','Netflix','JPMorgan','Visa','Walmart','Coca','Pepsi','Exxon','Chevron','Pfizer','Johnson','Boeing','Intel','AMD','Adobe','Salesforce','Oracle','Cisco','Disney','McDonald','Nike','Starbucks','Uber','PayPal','Berkshire','BankofAmerica','Goldman','Caterpillar','3M','HomeDepot','Costco','Procter','UnitedHealth','Merck','AbbVie','Broadcom','Qualcomm','Texas','IBM','Ford','GeneralMotors','GeneralElectric','Honeywell','Lockheed',
 'SAP','Siemens','Allianz','BASF','Volkswagen','BMW','Mercedes','Adidas','Bayer','DeutscheBank','LVMH','Total','Sanofi','Airbus','Nestle','Novartis','Roche','ASML','Shell','Unilever','AstraZeneca','HSBC','BP','Glaxo','RioTinto','Vodafone','Barclays']
sel=[]
for n in names:
    m=[s for s in st if n.lower() in s.name.lower().replace(' ','')]
    if m: sel.append(m[0])
print('選択',len(sel),[s.name for s in sel])
out={}
for s in sel:
    mt5.symbol_select(s.name,True); i=mt5.symbol_info(s.name)
    r=mt5.copy_rates_from_pos(s.name,mt5.TIMEFRAME_H1,0,99000)
    if r is None or len(r)<2000: continue
    df=pd.DataFrame(r); bid=i.bid if i.bid>0 else float(df.close.iloc[-1])
    out[s.name]={'h1':df,'spec':dict(path=s.path,swap_long=i.swap_long,swap_short=i.swap_short,swap_mode=i.swap_mode,cur=i.currency_profit,vmin=i.volume_min,contract=i.trade_contract_size,bid=bid,point=i.point,spread=i.spread*i.point,margin=mt5.order_calc_margin(mt5.ORDER_TYPE_BUY,s.name,i.volume_min,i.ask if i.ask>0 else bid))}
mt5.shutdown(); pickle.dump(out,open('_xm_stocks_h1.pkl','wb'))
print('取得',len(out))
for k,v in list(out.items())[:5]: print(k,v['spec'],len(v['h1']),pd.to_datetime(v['h1'].time.iloc[0],unit='s').date())
