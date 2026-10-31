import os, pickle, pandas as pd, numpy as np, sys
os.chdir(r"C:\Users\星野\Documents\my-first-project")
for fn in ("_bars_daily_5y_1024.pkl", "_daily_jq_1002.pkl", "_jq_ext_cache_0924.pkl"):
    try:
        X = pickle.load(open(fn, "rb"))
    except Exception as e:
        print(fn, "ERR", e); continue
    print("==", fn, type(X))
    if isinstance(X, dict):
        print("  keys", list(X.keys())[:10])
        for k, v in X.items():
            if isinstance(v, dict):
                k2 = next(iter(v)); vv = v[k2]
                print("   ", k, "dict", len(v), "sample", k2, type(vv), getattr(vv, "columns", None) is not None and list(vv.columns), getattr(vv, "shape", None))
                if hasattr(vv, "index"): print("     idx", vv.index.min(), vv.index.max())
            elif isinstance(v, pd.DataFrame):
                print("   ", k, "DF", v.shape, list(v.columns)[:15]); print(v.head(2))
            else:
                print("   ", k, type(v), (len(v) if hasattr(v, "__len__") else v))
    elif isinstance(X, pd.DataFrame):
        print("  DF", X.shape, list(X.columns)[:20]); print(X.head(3))
        for c in ("Date", "date"):
            if c in X.columns: print("  dates", X[c].min(), X[c].max())
