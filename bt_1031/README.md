# 2026-10-30〜31 の検証一式（結論はメモリ/各RESULTS参照）
- fade5y/ … 今日の3本の代金線(vamin_bt.py→RESULTS_vamin.md: 3億のまま)・遊び1本(+12%指値25万: play12/play12s/play_k)
- bb_sigma/ … BBσ乖離の場中総当たり 276,480セル(RESULTS_bb_sigma.md: 全銘柄は負け・GU売り土俵でだけPF2.7だがBB不要)。grid_*.csv(66MB×2)は Downloads/trade_bt/bb_sigma_1031 に残置
- audit_fade/ … 今日の3本BTの監査(計算・先読み・約定・20年調整・IPO)。pklは残置
- audit_gokujo/ … 極上BTの監査(独立J-Quantsエンジンで再現・台帳照合)。pklは残置
- ルート _bt_sell_entry_grid_1031 … 売りスイング入口48通り(現行1位→その後廃止)
- ルート _bt_gokujo_score_grid_1031 … 極上選定スコア1,980通り(現行6位・上位5は雑音内→据え置き)
