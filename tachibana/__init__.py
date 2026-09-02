"""立花証券 e支店・API (v4r10) クライアント。

使い方（情報取得のみ）:
    from tachibana import TachibanaClient
    tc = TachibanaClient()          # .tachibana/ の認証ID・秘密鍵を読む
    tc.ensure_session()             # 当日の仮想URL（1日券）が無ければログイン
    print(tc.date_info())
    print(tc.market_price(["7203", "6501"]))
    df = tc.price_history("7203")   # 20年日足

注文系は TACHIBANA_ALLOW_ORDERS=1 を明示しない限り実行しない。
"""
from .client import (
    TachibanaClient,
    TachibanaError,
    TachibanaTransportError,
    TachibanaProtocolError,
    TachibanaSessionError,
    TachibanaResultError,
    PRICE_COLUMNS,
    history_to_frame,
    history_to_cache_frame,
)

__all__ = [
    "TachibanaClient",
    "TachibanaError",
    "TachibanaTransportError",
    "TachibanaProtocolError",
    "TachibanaSessionError",
    "TachibanaResultError",
    "PRICE_COLUMNS",
    "history_to_frame",
    "history_to_cache_frame",
]
