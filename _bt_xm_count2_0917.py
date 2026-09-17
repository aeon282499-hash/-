import pandas as pd, numpy as np
exec(open('_bt_xm_count_0917.py',encoding='utf-8').read().split("cur_j=LJ")[0])
m=(LG.prev>0)&(LG.dow==1)
print('GER40 火曜JST(欧州月曜夜)で直前>0の玉(=無条件化で増える玉):',stats(LG.net[m]))
y=LG.net[m].groupby(LG.net[m].index.year).agg(['count','mean','sum']); print(y.round(3).to_string())
