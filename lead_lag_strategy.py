"""
部分空間正則化付きPCAを用いた日米業種リードラグ投資戦略
Lead-lag strategy using subspace regularization PCA

論文: 中川慧 et al. (2026) SIG-FIN-036
"""

import numpy as np
import pandas as pd
import yfinance as yf
from scipy.linalg import eigh
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')

# ============================================================
# 設定
# ============================================================
US_TICKERS  = ['XLB', 'XLC', 'XLE', 'XLF', 'XLI', 'XLK', 'XLP', 'XLRE', 'XLU', 'XLV', 'XLY']
JP_TICKERS  = ['1617.T','1618.T','1619.T','1620.T','1621.T','1622.T','1623.T',
               '1624.T','1625.T','1626.T','1627.T','1628.T','1629.T','1630.T',
               '1631.T','1632.T','1633.T']
ALL_TICKERS = US_TICKERS + JP_TICKERS
NU = len(US_TICKERS)
NJ = len(JP_TICKERS)
N  = NU + NJ

# ハイパーパラメータ（論文通り）
L   = 60    # 推定ウィンドウ長（営業日）
K   = 3     # 抽出する共通因子数
LAM = 0.9   # 正則化強度
Q   = 0.3   # ロング・ショートの分位点

# サンプル期間
START      = '2010-01-01'
END        = '2025-12-31'
CFULL_END  = '2014-12-31'   # Cfull（事前相関）推定期間

# シクリカル・ディフェンシブ分類（論文表記）
US_CYCLICAL  = ['XLB', 'XLE', 'XLF', 'XLRE']
US_DEFENSIVE = ['XLK', 'XLP', 'XLU', 'XLV']
JP_CYCLICAL  = ['1618.T', '1625.T', '1629.T', '1631.T']
JP_DEFENSIVE = ['1617.T', '1621.T', '1627.T', '1630.T']


# ============================================================
# データ取得
# ============================================================
def download_data():
    print("データ取得中 (yfinance)...")

    close = yf.download(ALL_TICKERS, start=START, end=END,
                        auto_adjust=True, progress=False)['Close']
    open_ = yf.download(JP_TICKERS,  start=START, end=END,
                        auto_adjust=True, progress=False)['Open']

    close.index = pd.to_datetime(close.index).tz_localize(None)
    open_.index = pd.to_datetime(open_.index).tz_localize(None)

    print(f"  Close: {close.shape}, Open(JP): {open_.shape}")
    return close, open_


def compute_returns(close, open_):
    """Close-to-Close（全銘柄）と Open-to-Close（日本のみ）を計算"""
    rcc = close[ALL_TICKERS].pct_change()

    roc = pd.DataFrame(index=close.index, columns=JP_TICKERS, dtype=float)
    for t in JP_TICKERS:
        roc[t] = (close[t] - open_[t]) / open_[t]

    return rcc, roc


# ============================================================
# 事前部分空間の構築
# ============================================================
def gram_schmidt_orthogonalize(v, basis):
    """vをbasisの既存列に対してGram-Schmidt直交化"""
    for b in basis:
        v = v - np.dot(v, b) * b
    norm = np.linalg.norm(v)
    if norm < 1e-10:
        raise ValueError("直交化後のベクトルがゼロ")
    return v / norm


def build_prior_subspace():
    """事前固有ベクトル V0 ∈ R^{N x K0} を構築（K0=3）"""
    basis = []

    # v1: グローバルファクター（全銘柄等ウェイト）
    v1 = np.ones(N) / np.sqrt(N)
    basis.append(v1)

    # v2: 国スプレッド（米国+, 日本-）
    v2_raw = np.zeros(N)
    v2_raw[:NU] =  1.0 / np.sqrt(NU)
    v2_raw[NU:] = -1.0 / np.sqrt(NJ)
    v2 = gram_schmidt_orthogonalize(v2_raw, basis)
    basis.append(v2)

    # v3: シクリカル・ディフェンシブ
    v3_raw = np.zeros(N)
    for i, t in enumerate(ALL_TICKERS):
        if t in US_CYCLICAL or t in JP_CYCLICAL:
            v3_raw[i] =  1.0
        elif t in US_DEFENSIVE or t in JP_DEFENSIVE:
            v3_raw[i] = -1.0
    v3 = gram_schmidt_orthogonalize(v3_raw, basis)
    basis.append(v3)

    V0 = np.column_stack(basis)   # N x 3
    return V0


def build_C0(Cfull, V0):
    """事前エクスポージャー行列 C0 を構築（論文 式10-12）"""
    D0    = np.diag(V0.T @ Cfull @ V0)
    Craw0 = V0 @ np.diag(D0) @ V0.T

    diag      = np.diag(Craw0)
    diag[diag <= 0] = 1e-10
    scale     = 1.0 / np.sqrt(diag)
    C0        = np.outer(scale, scale) * Craw0
    np.fill_diagonal(C0, 1.0)
    return C0


# ============================================================
# シグナル計算（論文 式13-19）
# ============================================================
def compute_signal(Z_window, zU_today, C0):
    """
    Z_window  : L x N  推定ウィンドウの標準化リターン
    zU_today  : NU     当日米国標準化リターン
    C0        : N x N  事前相関行列
    戻り値    : NJ     日本セクターの翌日予測シグナル
    """
    L_w = Z_window.shape[0]

    # ウィンドウ相関行列
    Ct  = Z_window.T @ Z_window / max(L_w - 1, 1)
    np.fill_diagonal(Ct, 1.0)

    # 正則化（式13）
    Creg = (1 - LAM) * Ct + LAM * C0

    # 固有分解（eigh: 対称行列専用、安定）
    eigvals, eigvecs = eigh(Creg)
    idx    = np.argsort(eigvals)[::-1]
    Vk     = eigvecs[:, idx[:K]]   # N x K

    VU = Vk[:NU, :]   # NU x K
    VJ = Vk[NU:, :]   # NJ x K

    # 式18-19
    ft     = VU.T @ zU_today   # K
    signal = VJ @ ft            # NJ
    return signal


# ============================================================
# バックテスト
# ============================================================
def backtest(rcc, roc):
    # 日付インデックスを揃える
    all_dates = rcc.index.union(roc.index)
    rcc = rcc.reindex(all_dates).ffill(limit=1)
    roc = roc.reindex(all_dates)

    rcc_arr = rcc[ALL_TICKERS].values.astype(float)
    roc_arr = roc[JP_TICKERS].values.astype(float)
    dates   = rcc.index

    # ---- Cfull を2010-2014で推定 ----
    mask_full = dates <= pd.Timestamp(CFULL_END)
    Z_full_df = rcc.loc[mask_full, ALL_TICKERS].dropna()
    mu0  = Z_full_df.mean().values.copy()
    std0 = Z_full_df.std().values.copy()
    std0[std0 < 1e-10] = 1e-10
    Z_full_std = ((Z_full_df.values - mu0) / std0)
    Cfull = Z_full_std.T @ Z_full_std / max(len(Z_full_std) - 1, 1)
    np.fill_diagonal(Cfull, 1.0)
    print(f"Cfull 推定: {Z_full_df.index[0].date()} 〜 {Z_full_df.index[-1].date()}  ({len(Z_full_df)}日)")

    # ---- 事前部分空間 ----
    V0 = build_prior_subspace()
    C0 = build_C0(Cfull, V0)

    # ---- ローリング ----
    n_dates  = len(dates)
    results  = []
    start_bt = pd.Timestamp(CFULL_END) + pd.Timedelta(days=1)

    print("バックテスト実行中...")
    for i in range(L + 1, n_dates - 1):
        t_date = dates[i]
        if t_date < start_bt:
            continue

        # ウィンドウ [i-L, i-1]
        window = rcc_arr[i - L: i, :]
        if np.isnan(window).any():
            continue

        # 当日の米国リターン
        us_today = rcc_arr[i, :NU]
        if np.isnan(us_today).any():
            continue

        # 翌日の日本 Open-to-Close
        jp_next = roc_arr[i + 1, :]
        if np.isnan(jp_next).any():
            continue

        # 標準化（ウィンドウの統計量を使用）
        mu  = window.mean(axis=0)
        std = window.std(axis=0)
        std[std < 1e-10] = 1e-10

        Z_win    = (window   - mu)    / std
        zU_today = (us_today - mu[:NU]) / std[:NU]

        # シグナル計算
        signal = compute_signal(Z_win, zU_today, C0)

        # ロング・ショートポートフォリオ（式3-7）
        n_long  = max(1, int(np.floor(NJ * Q)))
        n_short = max(1, int(np.floor(NJ * Q)))

        rank       = np.argsort(signal)[::-1]
        long_idx   = rank[:n_long]
        short_idx  = rank[-n_short:]

        w          = np.zeros(NJ)
        w[long_idx]  =  1.0 / n_long
        w[short_idx] = -1.0 / n_short

        ret = float(np.dot(w, jp_next))
        results.append({'date': dates[i + 1], 'return': ret})

    df = pd.DataFrame(results).set_index('date')
    print(f"完了: {len(df)} 営業日")
    return df


# ============================================================
# パフォーマンス評価
# ============================================================
def evaluate(returns_df, name='Strategy'):
    r = returns_df['return'].dropna()
    AR   = r.mean() * 252
    RISK = r.std()  * np.sqrt(252)
    RR   = AR / RISK if RISK > 0 else 0

    cum          = (1 + r).cumprod()
    rolling_max  = cum.cummax()
    MDD          = (cum / rolling_max - 1).min()

    print(f"\n{'='*40}")
    print(f"  {name}")
    print(f"{'='*40}")
    print(f"  年率リターン  : {AR*100:+.2f}%")
    print(f"  年率リスク    : {RISK*100:.2f}%")
    print(f"  R/R           : {RR:.2f}")
    print(f"  最大DD        : {MDD*100:.2f}%")
    print(f"  観測日数      : {len(r)}")

    return {'name': name, 'AR': AR, 'RISK': RISK, 'RR': RR, 'MDD': MDD, 'cum': cum}


def plot_results(metrics_list):
    plt.figure(figsize=(12, 5))
    for m in metrics_list:
        plt.plot(m['cum'].index, m['cum'].values, label=m['name'])
    plt.title('累積リターン（日米業種リードラグ戦略）')
    plt.xlabel('日付')
    plt.ylabel('累積リターン')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig('cumulative_returns.png', dpi=150)
    print("\ncumulative_returns.png に保存しました")


# ============================================================
# ターンオーバー集計（コスト試算用）
# ============================================================
def compute_turnover(returns_df, rcc, roc):
    """ポジションの日々の変化率（ターンオーバー）を計算"""
    # 簡易版: シグナルのランキング変化日数を数える
    # backtest内でシグナルを再計算するのはコストが高いため省略
    # 代わりにリターン系列からの間接推計
    r = returns_df['return'].dropna()
    annual_days = 252
    gross_exposure = 2.0   # |w| = 2

    # スプレッドコストの試算
    for spread_pct in [0.03, 0.07, 0.10]:
        daily_cost = gross_exposure * (spread_pct / 100)  # 往復
        annual_cost = daily_cost * annual_days
        net_ar = r.mean() * annual_days - annual_cost
        print(f"  スプレッド {spread_pct:.2f}% → 年間コスト {annual_cost*100:.1f}%  "
              f"| 推定純利回り {net_ar*100:+.1f}%")


# ============================================================
# メイン
# ============================================================
if __name__ == '__main__':
    # 1. データ取得
    close, open_ = download_data()
    rcc, roc     = compute_returns(close, open_)

    # 2. PCA SUB バックテスト
    pca_sub = backtest(rcc, roc)
    m_sub   = evaluate(pca_sub, 'PCA SUB（論文手法）')

    # 3. コスト試算
    print("\n---- コスト試算（300万円想定）----")
    compute_turnover(pca_sub, rcc, roc)

    # 4. 結果保存
    pca_sub.to_csv('strategy_returns.csv')
    print("\nstrategy_returns.csv に保存しました")

    # 5. グラフ
    plot_results([m_sub])
