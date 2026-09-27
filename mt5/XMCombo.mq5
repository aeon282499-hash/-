//+------------------------------------------------------------------+
//|                                                      XMCombo.mq5 |
//|  星野さん用 XM Zero口座 統合EA（2026-09-08）                        |
//|   A) 金 15分ショート: ロンドン10:15売り→10:30買戻し(英国DST追従)     |
//|      Zero実測 spread$0.14+手数料$0.07・net+0.09$/oz/回・SL$10        |
//|   B) 日経225 夜ドリフト: 15:00JST買い→翌9:00JST売り・月〜木          |
//|      XM実データ2016-26 net+0.041%/晩・年+8〜10%・勝ち年8/11          |
//|  1チャートで両方を動かす（[StartUp]で貼れるEAが1本のため統合）。      |
//+------------------------------------------------------------------+
#property copyright "hoshino"
#property version   "1.00"
#property strict
#include <Trade\Trade.mqh>

//--- 共通
input bool   InpDemoOnly      = false;      // true=デモ口座以外では発注しない
input double InpStopBelowBalance = 25000;   // 残高がこの円を割ったら新規建てを全停止(0=無効)・人が判断するまで再開しない
//--- A) 金 15分ショート
input bool   InpGoldOn        = true;       // 金ショートを動かす
input string InpGoldSymbol    = "GOLD.";    // 銘柄(Zero口座は GOLD.)
input double InpGoldJpyPer001 = 25000;      // 0.01lotあたりの必要残高(円)。9/17: ゲート込みBSで5万→2.5万はE[log]+0.65・下位10%/DD/停止は不変(ゲートが悪い時期を切るため)・次段1.25万は実弾のゲート成績30回で判断
input double InpGoldLotMax    = 1.00;       // 上限ロット
input double InpGoldStopUsd   = 10.0;       // 損切り幅($/oz・0=無し)
input double InpGoldCostUsd   = 0.593;      // ゲートの紙成績から引く往復コスト($/oz)。9/23: 実約定から逆算(スプレッド0.14+手数料0.453)。旧値0.21のままだと閾値0.00(=$0.593で校正)よりゲートが$0.38/oz緩くなる(レビュー指摘)
input int    InpGoldMaxSpread = 40;         // 許容スプレッド(pt=0.01$)
input int    InpGoldHourLon   = 10;         // 売り時刻 ロンドン(時)
input int    InpGoldMinLon    = 19;       // 売り時刻 ロンドン(分)。9/22夜: 15→19。決済10:32固定で建てを1分刻みに振ると、金額は10:15が最大だが校正済みポートフォリオシム(E[log]×全停止0%)では10:19が最良=同じ金2.5万サイズで1年中央7.7→9.2万(+19%)・DD-73→-69%・E[log]は同等。t も4.93→5.74と上昇(建てを遅らせるほど単調に上がる)。本人の裁量も18:19-18:28で6戦6勝と一致
input int    InpGoldHoldMin   = 13;       // 保有分数。建て10:19・決済10:32(=値決め10:30の2分後・1分解剖で累計がここでピーク)
input long   InpGoldMagic     = 20260908;
input int    InpGoldGateN     = 40;         // 自己判断ゲート: 直近N回(紙でも毎日計測)の平均$/ozが閾値超の時だけ撃つ(0=無効)。BT: 10年+233→+539$・2016-24 -217→+92
input double InpGoldGateThr   = 0.00;       // 閾値 $/oz(AM用)。9/22夜: 実勢コスト$0.593(実約定から逆算・BT前提$0.21の2.8倍)×XM自身のM1 11.3年で引き直し。0.10→0.00で 11年+99,108→+107,643円・t+4.02→+4.31・前期t+1.4→+2.1・勝ち年4→5/12・最大DD-11,706→-8,567(全軸で改善)。※ゲート無しは実勢コストだと-109,823円/t-5.12＝ゲートは必須
input double InpGoldFriMult   = 0.5;       // 金AMの金曜だけロット倍率。9/22夜: 新窓(10:19→10:32)で曜日別を測ると金曜は1回+0.199$/oz vs 他+1.096(差のt-2.40)と有意に弱い。x0.5でt+5.74→+5.82・金額はほぼ同じ。※ロット刻み0.01なので小さい残高では四捨五入で0.02等になる
input bool   InpSkipUkHoliday = true;      // 英国の銀行休業日はLBMAオークション自体が開催されないので撃たない。9/22夜: 除外で11年+116,274→+124,065円・t+5.74→+6.28・前期t+2.5→+2.9・最大DD-7,887→-5,178(全軸改善)。該当は594玉中14玉。窓のティック数も休業日686 vs 通常1054で「オークション無し」と整合。米国祝日は逆に除外すると悪化(t5.52)するので対象外
input double InpGoldMonMult   = 2.0;       // 金AMの月曜だけロット倍率。9/22: 値決めフローは週末明けが最大＝ゲート無し全2,751玉で月+0.549$/oz vs 他+0.014(差のt+3.47)・前後半とも同符号(+1.5/+2.4)・隣接窓でも再現。2倍で10年+101,164→+145,051円/勝ち年4→5/9/t5.64→5.67。⚠️弱点=前半の差のt1.51・月曜の利益の80%が2025-26年→2027年に再判定[X4]
input double InpGoldVolOrThr  = 0;          // 9/23: ボラ併用ゲート(AM)。金の20日実現ボラ(確定D1終値・年率%)がこの値を超えたらP&Lゲートが休止でも撃つ(0=無効)。発見=値決めの落差は$/ozでボラに比例(20日ボラ四分位Q4だけ両半期プラス・r+0.28)・コストは$固定→高ボラ時は直近40回が負けていても期待値が正。閾値20〜28%が高原・22で11.3年+813→+886$/t6.31→6.51/DD-27,899→-22,374円/2026除外+222→+295$(t3.36→3.71)/最悪日不変。BT=ロンドン日足とサーバー日足で相関0.99・結果同一
input double InpGoldPmVolOrThr = 0;         // 同・PM用。25%で11.3年+385→+466$/t2.29→2.66/2026除外+204→+266$/DD-51,676→-57,724円(DDだけ悪化・PMはAMより弱い)
input double InpGoldPmUpSkipPct = 0;       // 9/24: PMだけ「建て時の価格が前日(サーバー日足)終値比+この%超なら撃たない」(0=無効)。_bt_gold_pm_upday_0924.py: +0.5%で11.3年+466→+568$/t2.62→3.58/DD-77→-53$/プラセボp=0.001・閾値+0.3〜+1.0%で全部+100〜148$。⚠️落ちる玉の損の大半は2026年(-96$)で2025年は逆(+51$)・2026除外の上乗せは+6$だけ。AMに同じ除外をかけると-196$(AMは上げ日でも勝つ)なのでPM専用
input double InpGoldPmGateThr = 0.00;       // 閾値 $/oz(PM用)。9/22: PMは0.00の方が11年+48,202→+52,820円・直近2年+42,500→+42,608円と両期間で上。AMは0.10のまま(0.00だと11年が落ちる)
input bool   InpGoldPmOn      = true;       // 9/22夜に再開: 一度OFFにしたのは私が誤った窓(14:55→15:05)で測ったため。正しい窓14:53→15:03は実勢コスト$0.593でも11年+82,600円/t+2.76(95セル中1位)。日本時間 夏22:53→23:03 / 冬23:53→00:03
input int    InpGoldPmHourLon = 14;         // 売り時刻 ロンドン(時)
input int    InpGoldPmMinLon  = 53;         // 売り時刻 ロンドン(分)。9/22夜: 55→53。AMと同じ文献由来の修正(公表後を持たない)。14:53→15:03 は11年+52,820→+89,479円(+69%)・直近2年+42,608→+79,448(+86%)・t2.21→3.27・勝ち年5→7/11・上位3日除去+34,525→+63,665。周囲±2分の24セル中19が現行超え=尾根(AMと違い幅がある)
input int    InpGoldPmHoldMin = 10;         // 保有分数。10分のまま(入口を14:53へ2分前倒ししたので出口は15:03)。9/22夜のグリッド上位は全部14:52-14:54入り×15:01-15:04出に固まる。※旧コメントの「PM10分は不可」は14:50→15:00の話で別窓
input long   InpGoldPmMagic   = 20260919;
//--- B) 日経225 夜ドリフト
//--- 9/19 本人「徐々にロット増やして・フル全自動」: 残高ラダー。残高がしきい値以上なら各レッグの単位残高を÷k(=枚数×k)。BS(修正コスト・金ゲート込み): ×1固定E[log]5.09/DD-54% → ≥10万×1.25 5.71/DD-63% → ≥30万×1.5 6.01/DD-74%・停止0%。×2はE[log]低下(オーバーKelly)なので3段目は既定off
input double InpScale1Bal     = 100000;     // 残高がこれ以上で単位×InpScale1(0=無効)
input double InpScale1        = 1.25;
input double InpScale2Bal     = 300000;     // 残高がこれ以上で単位×InpScale2(0=無効)
input double InpScale2        = 1.5;
input double InpScale3Bal     = 0;          // 3段目(0=無効)。×2.0はBSでE[log]4.79に低下＝入れない
input double InpScale3        = 2.0;

input bool   InpJpOn          = true;       // 日経夜ドリフトを動かす
input string InpJpSymbol      = "JP225Cash";
input double InpJpJpyPerLot   = 8750;       // 1.0lot(名目約6.5万円)あたりの必要残高(円)
input double InpJpLotMax      = 500.0;
input int    InpJpEntryHour   = 15;         // 買い時刻 JST。9/22夜に14時へ変更したが同日中に差し戻し: 変更根拠だった朝のグリッドが「15→09だけ金曜建て(EAは建てない)を含む」不公平比較だった。EA準拠(月〜木)で測り直すと合算は3%差の団子で、校正済みポートフォリオシムでは15:00→09:00がE[log]+8.75/停止0%/5年下位10%28.6万 と最良(14:00→05:00は+8.02/停止4%/5.5万)
input int    InpJpExitHour    = 9;         // 手仕舞い時刻 JST。同上で09:00へ差し戻し。05:00(米株の引け)はスワップ0泊だがUS500の1回%が+0.0717→+0.0408と落ち、複利+破産込みで負ける。PrevNightPct のパラメータ化(9/22)はそのまま維持
input bool   InpJpHoldWeekend = false;      // 金曜も建てて月曜朝に閉じる
input bool   InpJpPrevNightFilter = true;   // 前夜(前日15:00→当日9:00)が上げなら見送る(15年+82→+99%・XM t=3.2・月勝率58→61%)
input double InpJpPrevNightMax = 0.0;       // 前夜の上げがこの%以下の日だけ建てる
input bool   InpJpMondayFree  = true;       // 月曜JSTはフィルタ無しで建てる(日経/US500共通・月曜夜は無条件でt4.1/3.5・フィルタは総利益を削るだけ・E[log]3.57→4.60)
input int    InpJpMaxSpread   = 20;         // 許容スプレッド(pt=1円)
input long   InpJpMagic       = 20260909;
input int    InpJpAddHour     = 1;          // 追加判定の時刻JST(翌日01:00)・0=無効
input double InpJpAddPct      = -0.5;       // 建値比がこの%以下なら同量を追加(BT: 01時≤-0.5% 残り区間+0.165%/回 t2.8 勝9/11・00-01時/-0.25〜-1.0で高原)
input double InpJpAddMult     = 1.0;        // 追加量(元玉の倍率)。9/23: 2.0→1.0(本人選択A)。9月を今のロットで回すと9/8・9/10の追加x2で-25,580/-53,684円(1晩で残高の半分)。x2はE[log]+0.36だが尻尾が太く、追加後43.8枚だと-2.6%で全停止に届く(9/10は-2.69%)

input bool   InpUsOn          = true;       // US系の夜ドリフト。9/22深夜: US500→US100(ナスダック100)に置換。時間軸ずらし地図(92,160セル)で床超えはUS100の22h保有だけ・校正済みE[log]シムでUS500(16→15)をUS100(21→翌19)に置換するとE[log]+6.62→+8.01/5年下位10%16→34万/1年下位10%6.0→6.5万・停止0%のまま。代償=DD中央-61→-85%(本人「儲かるなら実施」)
input string InpUsSymbol      = "US100Cash";       // 9/22深夜: US500Cash→US100Cash。手数料ゼロ・コスト率0.0095%(全銘柄最安)・US500との相関+0.77・金とは+0.11
input double InpUsJpyPer01    = 50000;      // 0.1lot(US100: 名目約3,050USD≒46万円)あたりの必要残高(円)。9/22深夜: US100置換のサイズ振り(30k〜100k)でE[log]は40k(+7.83)と50k(+7.81)が同率、50kの方が1年中央9.0万(40kは8.6)・DD-82%(40kは-85%)・5年中央11,272万で最良→50,000。現行(US500 16→15・40k)はE[log]+6.62/DD-61%/1年中央9.3万/1年下位10%6.0万。置換後は1年下位10%6.5万・停止0%のままだがDD中央-61→-82%が代償
input double InpUsLotMax      = 50.0;
input int    InpUsMaxSpread   = 600;       // 許容スプレッド(pt=0.01$)。US100の実測290pt(US500の80ptから引き上げ)
input int    InpUsEntryHour   = 21;       // US100の買い時刻 JST(=サーバー15:00・NY寄り前)。地図の最良セル 建て14:45〜15:15→翌13:00
input int    InpUsExitHour    = 19;       // US100の手仕舞い時刻 JST(翌日・=サーバー13:00)。保有22時間
input long   InpUsMagic       = 20260924;       // US100レッグ用に更新(US500時代の20260913と履歴を分ける)

input bool   InpDeOn          = true;       // GER40 欧州の夜(01:00JST買→16:00JST売・火〜金JST・直前レッグ≤0)
input string InpDeSymbol      = "GER40Cash";
input double InpDeJpyPer01    = 65000;      // 0.1lot(名目約46万円)あたりの必要残高(円)。9/22: 同上でUS500と一緒に薄くした
input double InpDeLotMax      = 50.0;
input int    InpDeMaxSpread   = 400;        // 許容スプレッド(pt=0.01EUR)
input int    InpDeEntryHour   = 1;          // 買い時刻 JST
input int    InpDeExitHour    = 16;         // 手仕舞い時刻 JST
input long   InpDeMagic       = 20260914;
input bool   InpDePrevWeekend = false;      // 9/26: 月曜夜(JST火曜01:00建て)の判定に週末(土01:00→月16:00JST)の動きを使う。false=従来(26hルールで週末を飛ばし木曜夜のレッグで判定)。_bt_xm_monday_wkd_0926.py/_ger_monday_robust: 従来の火曜は+0.059%/回 t1.20(後半t-0.52・週末≤0との一致率0.497=実質ランダム)→週末≤0で+0.154% t2.69(前2.30/後1.41・勝ち年8/10・同数ランダム1000回でp=0.013・外れる側は+0.004%)。レッグ全体873回+45.1%→857回+63.0%。弱点=2020年が利益の6割・上位3日除去t2.08

input bool   InpGdOn          = false;      // 9/22停止: αではなくβと判明。同じ時間の常時買い+463,940円/t2.90に対しレッグは+185,916/t1.57、しかも「直前レッグ≤0」で選んだ日の平均+0.917$/oz < 全日平均+1.446$/oz＝フィルタが平均より悪い日を選んでいる。残高12万で自動起動する前に切る
input double InpGdJpyPer001   = 120000;     // 0.01lot(=1oz・名目約65万円)あたりの必要残高(円)。残高12万未満は0枚
input double InpGdLotMax      = 1.00;
input int    InpGdMaxSpread   = 40;         // 許容スプレッド(pt=0.01$)
input int    InpGdEntryHour   = 4;          // 買い時刻 JST
input int    InpGdExitHour    = 22;         // 手仕舞い時刻 JST
input long   InpGdMagic       = 20260915;
//--- C) 金 Globex再開買い（サーバー01:05買→03:00売・13年t8.2・14/14年・0.01lot=名目68万円）
input int    InpGxMode        = 0;          // 9/22停止: 週明け/日替わり再開のクオート人工物と確定(24通貨ペア横断で同型・ペッグのEURDKKでt+15.6・USDHKDでt+13.2)。紙でも計測する意味なし          // 0=off / 1=紙(仮想約定をログとFilesに記録) / 2=実弾(残高がInpGxMinBalance以上のときだけ)
input double InpGxMinBalance  = 200000;     // 実弾に切り替える残高(円)・未満なら紙のまま
input int    InpGxEntryHourSrv = 1;         // 建て時刻 サーバー(時)・メンテ明け01:05
input int    InpGxEntryMinSrv  = 5;
input int    InpGxExitHourSrv  = 3;         // 決済時刻 サーバー(時)
input int    InpGxMaxSpread   = 25;         // 許容スプレッド(pt=0.01$)・超えたら最大60秒待つ
input double InpGxJpyPer001   = 20000;      // 0.01lotあたりの必要残高(円)
input double InpGxLotMax      = 0.10;
input long   InpGxMagic       = 20260910;
//--- D) 金 大台ブレイク（2026-09-27 G1発見・既定off＝本人承認後に_deploy_combo_pending.ps1でon）
//   大台($100の倍数)に「その日(サーバー日)初めて下から」触れたら、その+$1に買いの逆指値を置く→約定したら15分後に決済。
//   機構=Osler(2005): 大台のすぐ上に溜まった損切り(売り方)・ブレイク買いの逆指値が連鎖して加速する。
//   BT(_bt_gold_rn_break_0927.py・親の独立再現 _verify_gold_x00_0927.py): XM M1 2015-26 年25回・コスト抜き+0.0575%(t4.42)・
//   今のコスト率0.0138%後+0.0437%(≒$1.87/oz・t3.36)・前後半t2.20/2.61・11/12年・上位3玉除去t2.92・Dukascopyでt4.19。
//   $100格子を+$5〜95ずらした偽の格子は効果ゼロ(+$50でもコスト後t1.32・前半マイナス)＝大台そのものの効果。下抜けの売りは弱いので買いだけ。
//   ⚠️BTは逆指値の約定=大台+$1(窓で飛び越えた時は足の始値)。実弾の滑りは未測定→最初は小さいロットで実測する。
input bool   InpRnOn          = false;
input double InpRnStep        = 100.0;      // 大台の刻み($)
input double InpRnDelta       = 1.0;        // 上抜けの幅($)。BIDが大台+δ に届いたら買い(逆指値はASK基準なので+その時のスプレッド)
input int    InpRnArmMin      = 60;         // 大台に触れてから何分以内の上抜けだけ買うか(過ぎたら逆指値を取り消す)
input int    InpRnHoldMin     = 15;         // 保有分数(時間決済)
input double InpRnStopUsd     = 10.0;       // 損切り幅($/oz・0=無し)。BTは無しが最良だが$10でもt2.69・発動11%→事故の上限として付ける
input double InpRnLot         = 0.02;       // 固定ロット(G1推奨: 最初は0.02〜0.03・0.10だと2026年DD-10万円)
input int    InpRnMaxSpread   = 40;         // 許容スプレッド(pt=0.01$)
input int    InpRnStartSrvMin = 120;        // 触れた判定と建ての時間帯(サーバー時刻の分): 02:00〜
input int    InpRnEndSrvMin   = 1364;       // 〜22:44(手仕舞いが23:00のロールオーバー帯に入らないように)
input long   InpRnMagic       = 20260927;

//--- E) 金 ドル衝撃（2026-09-28 G3・既定off＝配備は _deploy_combo_pending.ps1）
//   EURUSD/USDCHF/USDJPY の15分の対数変化を、過去20暦日(28,800分・その分は含まない・3000値以上)の15分変化の標準偏差で割って z。
//   USD合成 zu=(−zEURUSD＋zUSDCHF＋zUSDJPY)/3。3つの向きがそろい |zu|≥4 の分を「発生」とし(15分以内に続く発生は最初だけ)、
//   金の15分変化が同じ向き(ドル安なら上)に動き始めていたら、次の分に金をその向きに建て、30分後に成行で決済。
//   サーバー23:00〜02:00にかかる取引・金の足が30分以上途切れた後の60分は撃たない。
//   BT(_bt_gold_usdshock_robust_0927.py・XM 2015-26): n1838 粗+0.0366%(t4.90)・実コスト後+0.0195%(t2.61)。
//   独立の確認(Dukascopy 2006-14・xm-gold-ea/research/r3_usdshock_confirm.py): n1250 粗+0.0430%(t4.25)・今のコスト率後t2.88・8/9年。
//   売り(ドル高→金売り)は2期間とも上位の日を抜くとゼロ → 既定は買いだけ(InpDsSide=1)。効き目はドル由来の動きだけ(金自身の急変は続かない)。
input int    InpDsMode        = 0;          // 0=off / 1=紙(判定と仮想の損益をログとFilesに記録) / 2=実弾
input int    InpDsSide        = 1;          // 1=買いだけ / 0=両方 / -1=売りだけ
input double InpDsLot         = 0.01;       // 固定ロット
input double InpDsK           = 4.0;        // |zu| のしきい値
input int    InpDsLookMin     = 15;         // 変化を見る分数
input int    InpDsHoldMin     = 30;         // 保有分数(建てた分の頭から)
input int    InpDsDeclMin     = 15;         // この分数以内に続く発生は同じ塊(最初だけ撃つ)
input int    InpDsWinMin      = 28800;      // 標準偏差の窓(分)=20暦日
input int    InpDsMinN        = 3000;       // 標準偏差に要る値の数
input double InpDsStopUsd     = 0.0;        // 事故用の損切り($/oz・0=無し)
input int    InpDsMaxSpread   = 40;         // 許容スプレッド(pt=0.01$)
input int    InpDsValidStartMin = 120;      // 発生として数える時間帯(サーバー時刻の分): 02:00〜
input int    InpDsValidEndMin   = 1379;     // 〜22:59
input int    InpDsTradeEndMin   = 1349;     // 撃つのはこの分まで(保有30分が23:00にかからない: 22:29)
input int    InpDsSelfTestMin = 0;          // 起動時に過去この本数の金の分足で z を計算して Files の XMCombo_ds_selftest.csv に書く(Pythonの双子と突き合わせる検証用・0=しない)
input string InpDsEurUsd      = "EURUSD.";
input string InpDsUsdChf      = "USDCHF.";
input string InpDsUsdJpy      = "USDJPY.";
input long   InpDsMagic       = 20260928;

CTrade   trade;
datetime g_jpEntryDay = 0, g_jpExitDay = 0, g_usEntryDay = 0, g_usExitDay = 0, g_deEntryDay = 0, g_deExitDay = 0, g_jpAddDay = 0, g_gdEntryDay = 0, g_gdExitDay = 0;
datetime g_gxDay = 0; double g_gxPaperEntry = 0.0; datetime g_gxPaperTime = 0; bool g_gxPaperOpen = false; datetime g_gxWaitStart = 0;

datetime DayOf(datetime t) { return t - (t % 86400); }
datetime NowJST() { return TimeGMT() + 9 * 3600; }
bool UkDst(datetime gmt)
{
   MqlDateTime d; TimeToStruct(gmt, d); int y = d.year;
   datetime mar31 = StringToTime(StringFormat("%04d.03.31 01:00", y)); MqlDateTime m; TimeToStruct(mar31, m);
   datetime start = mar31 - m.day_of_week * 86400;
   datetime oct31 = StringToTime(StringFormat("%04d.10.31 01:00", y)); MqlDateTime o; TimeToStruct(oct31, o);
   datetime end = oct31 - o.day_of_week * 86400;
   return (gmt >= start && gmt < end);
}
datetime NowLondon() { datetime g = TimeGMT(); return g + (UkDst(g) ? 3600 : 0); }
#define NA_PCT (-999.0)   // 足が取れない(未ロード)印。0.0=「建てる」と区別する
//--- 他銘柄のH1/M1を毎ティック触って常時ロードしておく(判定時刻に初めて触ると -1 が返り、9/9-9/10は前夜が+0.00%扱いで素通りしていた)
void WarmSeries()
{
   if(InpJpOn) iTime(InpJpSymbol, PERIOD_H1, 1);
   if(InpUsOn) iTime(InpUsSymbol, PERIOD_H1, 1);
   if(InpDeOn) iTime(InpDeSymbol, PERIOD_H1, 1);
   if(InpGoldOn || InpGoldPmOn || InpGdOn || InpRnOn) { iTime(InpGoldSymbol, PERIOD_H1, 1); iTime(InpGoldSymbol, PERIOD_M1, 1); }
}
//--- 足が取れない時: 建て時刻から10分間は5秒ごとに再試行(true=まだ待つ)、過ぎたらフィルタ無しで建てる(false)
datetime g_naLogMin = 0;
bool NaRetry(string tag, int entryH, MqlDateTime &dt, datetime jst)
{
   if(dt.hour == entryH && dt.min < 10)
   {
      datetime m = jst - (jst % 60);
      if(g_naLogMin != m) { PrintFormat("[%s] 足が未ロード(H1取得-1) → 再試行中", tag); g_naLogMin = m; }
      return true;
   }
   PrintFormat("[%s] 足が10分取れないのでフィルタ無しで建てる", tag);
   return false;
}

bool AnotherInstanceRunning()
{
   long me = ChartID();
   for(long id = ChartFirst(); id >= 0; id = ChartNext(id))
      if(id != me && ChartGetString(id, CHART_EXPERT_NAME) == MQLInfoString(MQL_PROGRAM_NAME)) return true;
   return false;
}
bool CanTrade() { return !(InpDemoOnly && AccountInfoInteger(ACCOUNT_TRADE_MODE) != ACCOUNT_TRADE_MODE_DEMO); }
datetime g_haltLogDay = 0;
bool Halted()
{
   if(InpStopBelowBalance <= 0 || AccountInfoDouble(ACCOUNT_BALANCE) >= InpStopBelowBalance) return false;
   datetime today = DayOf(NowJST());
   if(g_haltLogDay != today) { PrintFormat("⛔ 残高%.0f円 < 全停止ライン%.0f円: 新規建てを停止中（既存建玉の決済だけ行う）", AccountInfoDouble(ACCOUNT_BALANCE), InpStopBelowBalance); g_haltLogDay = today; }
   return true;
}

bool HasPos(string sym, long magic)
{
   for(int i = PositionsTotal() - 1; i >= 0; i--)
   {
      ulong tk = PositionGetTicket(i);
      if(tk > 0 && PositionSelectByTicket(tk) && PositionGetString(POSITION_SYMBOL) == sym && PositionGetInteger(POSITION_MAGIC) == magic) return true;
   }
   return false;
}
void CloseAll(string sym, long magic, string tag)
{
   for(int i = PositionsTotal() - 1; i >= 0; i--)
   {
      ulong tk = PositionGetTicket(i);
      if(tk > 0 && PositionSelectByTicket(tk) && PositionGetString(POSITION_SYMBOL) == sym && PositionGetInteger(POSITION_MAGIC) == magic)
      {
         trade.SetExpertMagicNumber(magic);
         if(!trade.PositionClose(tk)) PrintFormat("[%s] 決済失敗 #%I64u ret=%d %s", tag, tk, trade.ResultRetcode(), trade.ResultRetcodeDescription());
         else PrintFormat("[%s] 決済 #%I64u @%.2f", tag, tk, trade.ResultPrice());
      }
   }
}
double g_lastK = 0;
double LotGoldK(double k);
double RealizedVol20(string sym);
double LotIdxK(string sym, double jpyPerUnit, double unit, double lotMax, double k);
double ScaleK()
{
   double b = AccountInfoDouble(ACCOUNT_BALANCE), k = 1.0;
   if(InpScale1Bal > 0 && b >= InpScale1Bal) k = InpScale1;
   if(InpScale2Bal > 0 && b >= InpScale2Bal) k = InpScale2;
   if(InpScale3Bal > 0 && b >= InpScale3Bal) k = InpScale3;
   if(k < 1.0) k = 1.0;
   if(k != g_lastK) { PrintFormat("[ラダー] 残高%.0f円 → 単位×%.2f(枚数%.2f倍) 金%.2f 日経%.1f US500%.1f GER40%.1f", b, k, k, LotGoldK(k), LotIdxK(InpJpSymbol, InpJpJpyPerLot, 1.0, InpJpLotMax, k), LotIdxK(InpUsSymbol, InpUsJpyPer01, 0.1, InpUsLotMax, k), LotIdxK(InpDeSymbol, InpDeJpyPer01, 0.1, InpDeLotMax, k)); g_lastK = k; }
   return k;
}
double LotGoldK(double k)
{
   double lot = MathFloor(AccountInfoDouble(ACCOUNT_BALANCE) / (InpGoldJpyPer001 / k)) * 0.01;
   double vmin = SymbolInfoDouble(InpGoldSymbol, SYMBOL_VOLUME_MIN), vstep = SymbolInfoDouble(InpGoldSymbol, SYMBOL_VOLUME_STEP);
   lot = MathMax(vmin, MathMin(InpGoldLotMax, lot)); return NormalizeDouble(MathFloor(lot / vstep) * vstep, 2);
}
double LotGold() { return LotGoldK(ScaleK()); }
//--- 前夜リターン: 当日9:00JSTのH1始値 ÷ 前日15:00JSTのH1始値 - 1（%）。取れなければ NA_PCT(呼び側で再試行)
double PrevNightPct(string sym, int eh, int xh)
{
   datetime srvOff = TimeTradeServer() - TimeGMT();          // サーバー時刻 - GMT
   datetime jst = NowJST(); datetime dayJ = DayOf(jst);
   datetime t9  = dayJ + xh * 3600 - 9 * 3600 + srvOff;   // 当日の手仕舞い時刻JST をサーバー時刻に(9/22: 9固定をやめてレッグと揃えた。揃えないと直近2年で-59,812円)
   datetime t15 = dayJ - 86400 + eh * 3600 - 9 * 3600 + srvOff; // 前日の建て時刻JST
   MqlDateTime dw; TimeToStruct(jst, dw);
   if(dw.day_of_week == 1) t15 -= 3 * 86400;                  // 月曜は金曜の建て時刻(実際は月曜無条件なので参照されない)
   int b9 = iBarShift(sym, PERIOD_H1, t9, true), b15 = iBarShift(sym, PERIOD_H1, t15, true);
   if(b9 < 0 || b15 < 0) return NA_PCT;
   double o9 = iOpen(sym, PERIOD_H1, b9), o15 = iOpen(sym, PERIOD_H1, b15);
   if(o9 <= 0 || o15 <= 0) return NA_PCT;
   return (o9 / o15 - 1.0) * 100.0;
}

double LotIdxK(string sym, double jpyPerUnit, double unit, double lotMax, double k)
{
   jpyPerUnit /= k;
   double vmin = SymbolInfoDouble(sym, SYMBOL_VOLUME_MIN), vstep = SymbolInfoDouble(sym, SYMBOL_VOLUME_STEP);
   if(vstep <= 0) vstep = vmin;
   double lot = MathFloor(AccountInfoDouble(ACCOUNT_BALANCE) / jpyPerUnit * unit / vstep + 1e-9) * vstep;   // 残高÷単位残高×単位lot をvstep刻みで切り捨て
   lot = MathMax(vmin, MathMin(lotMax, lot)); return NormalizeDouble(lot, 2);
}
double LotIdx(string sym, double jpyPerUnit, double unit, double lotMax) { return LotIdxK(sym, jpyPerUnit, unit, lotMax, ScaleK()); }
double LotJp() { return LotIdx(InpJpSymbol, InpJpJpyPerLot, 1.0, InpJpLotMax); }
double LotUs() { return LotIdx(InpUsSymbol, InpUsJpyPer01, 0.1, InpUsLotMax); }
double LotDe() { return LotIdx(InpDeSymbol, InpDeJpyPer01, 0.1, InpDeLotMax); }
//--- 直前レッグ: 直近の exitH 足(now以前)と、その前の entryH 足。間隔が26h超(週末跨ぎ)なら一つ前のexitH足へ戻る。取れなければ0(=建てる)
double PrevLegPct(string sym, int entryH, int exitH, bool weekendOk = false)
{
   datetime srvOff = TimeTradeServer() - TimeGMT();
   datetime now = TimeTradeServer();
   for(int back = 0; back < 8; back++)
   {
      datetime dayJ = DayOf(NowJST()) - back * 86400;
      datetime tE = dayJ + exitH * 3600 - 9 * 3600 + srvOff;          // その日のexitH(JST)をサーバー時刻に
      if(tE > now) continue;
      if(iBars(sym, PERIOD_H1) <= 0) return NA_PCT;                    // 未ロード → 呼び側で再試行
      int bE = iBarShift(sym, PERIOD_H1, tE, true);
      if(bE < 0) continue;
      datetime tA = tE - (exitH - entryH) * 3600;                       // 同日 entryH
      if(entryH > exitH) tA -= 86400;
      int bA = iBarShift(sym, PERIOD_H1, tA, true);
      // weekendOk: 同日のentryH足が無い(=週明けで市場が閉まっていた)時は、その前の直近のentryH足まで戻る(最大3日)
      //   GER40の月16:00JST足なら 土01:00JST(=金曜夜の建て時刻) → 直前レッグ=週末の動き
      for(int k = 1; weekendOk && bA < 0 && k <= 3; k++) bA = iBarShift(sym, PERIOD_H1, tA - k * 86400, true);
      if(bA < 0) continue;
      double oA = iOpen(sym, PERIOD_H1, bA), oE = iOpen(sym, PERIOD_H1, bE);
      if(oA <= 0 || oE <= 0) continue;
      return (oE / oA - 1.0) * 100.0;
   }
   return 0.0;
}
double LotGd()
{
   double lot = MathFloor(AccountInfoDouble(ACCOUNT_BALANCE) / InpGdJpyPer001 + 1e-9) * 0.01;   // 12万未満は0
   double vstep = SymbolInfoDouble(InpGoldSymbol, SYMBOL_VOLUME_STEP); if(vstep <= 0) vstep = 0.01;
   lot = MathMin(InpGdLotMax, lot); return NormalizeDouble(MathFloor(lot / vstep + 1e-9) * vstep, 2);
}
void GdTick()
{
   datetime jst = NowJST(); MqlDateTime dt; TimeToStruct(jst, dt); datetime today = DayOf(jst);
   if(dt.hour >= InpGdExitHour && HasPos(InpGoldSymbol, InpGdMagic) && g_gdExitDay != today)
   { if(CanTrade()) CloseAll(InpGoldSymbol, InpGdMagic, "金昼"); if(!HasPos(InpGoldSymbol, InpGdMagic)) g_gdExitDay = today; else Print("[金昼] 決済が残っている → 5秒後に再試行"); return; }
   bool okDay = (dt.day_of_week >= 2 && dt.day_of_week <= 5);        // 火〜金JST(土曜JST04:00=金曜NY午後は決済足が無いので建てない)
   if(dt.hour >= InpGdEntryHour && dt.hour < InpGdExitHour && okDay && !HasPos(InpGoldSymbol, InpGdMagic) && g_gdEntryDay != today)
   {
      if(dt.hour >= InpGdEntryHour + 3) { PrintFormat("[金昼] %02d時以降なので今日は建てない", dt.hour); g_gdEntryDay = today; return; }
      double lots = LotGd();
      if(lots < 0.01) { PrintFormat("[金昼] 残高%.0f円 < %.0f円 なので0枚(見送り)", AccountInfoDouble(ACCOUNT_BALANCE), InpGdJpyPer001); g_gdEntryDay = today; return; }
      if(Halted()) { g_gdEntryDay = today; return; }
      int spread = (int)SymbolInfoInteger(InpGoldSymbol, SYMBOL_SPREAD);
      if(spread > InpGdMaxSpread) { PrintFormat("[金昼] スプレッド%dpt > %d 見送り(再試行)", spread, InpGdMaxSpread); return; }
      double pn = PrevLegPct(InpGoldSymbol, InpGdEntryHour, InpGdExitHour);
      if(pn == NA_PCT) { if(NaRetry("金昼", InpGdEntryHour, dt, jst)) return; pn = 0.0; }
      if(pn > 0.0) { PrintFormat("[金昼] 直前レッグ%+.2f%% > 0 なので見送り", pn); g_gdEntryDay = today; return; }
      PrintFormat("[金昼] 直前レッグ%+.2f%% → 建てる", pn);
      if(!CanTrade()) { PrintFormat("[金昼][デモ以外] 買いシグナル lot=%.2f（発注せず）", lots); g_gdEntryDay = today; return; }
      trade.SetExpertMagicNumber(InpGdMagic);
      if(trade.Buy(lots, InpGoldSymbol, 0, 0, 0, "goldday")) PrintFormat("[金昼] 買い lot=%.2f(残高%.0f円) @%.2f spread=%dpt", lots, AccountInfoDouble(ACCOUNT_BALANCE), trade.ResultPrice(), spread);
      else PrintFormat("[金昼] 買い失敗 ret=%d %s", trade.ResultRetcode(), trade.ResultRetcodeDescription());
      g_gdEntryDay = today;
   }
}
void DeTick()
{
   datetime jst = NowJST(); MqlDateTime dt; TimeToStruct(jst, dt); datetime today = DayOf(jst);
   if(dt.hour >= InpDeExitHour && HasPos(InpDeSymbol, InpDeMagic) && g_deExitDay != today)
   { if(CanTrade()) CloseAll(InpDeSymbol, InpDeMagic, "GER40"); if(!HasPos(InpDeSymbol, InpDeMagic)) g_deExitDay = today; else Print("[GER40] 決済が残っている → 5秒後に再試行"); return; }
   bool okDay = (dt.day_of_week >= 2 && dt.day_of_week <= 5);        // 火〜金JST(=欧州の月〜木の夜)
   if(dt.hour >= InpDeEntryHour && dt.hour < InpDeExitHour && okDay && !HasPos(InpDeSymbol, InpDeMagic) && g_deEntryDay != today)
   {
      if(dt.hour >= InpDeEntryHour + 3) { PrintFormat("[GER40] %02d時以降なので今日は建てない", dt.hour); g_deEntryDay = today; return; }
      if(Halted()) { g_deEntryDay = today; return; }
      int spread = (int)SymbolInfoInteger(InpDeSymbol, SYMBOL_SPREAD);
      if(spread > InpDeMaxSpread) { PrintFormat("[GER40] スプレッド%dpt > %d 見送り(再試行)", spread, InpDeMaxSpread); return; }
      double pn = PrevLegPct(InpDeSymbol, InpDeEntryHour, InpDeExitHour, InpDePrevWeekend);
      if(pn == NA_PCT) { if(NaRetry("GER40", InpDeEntryHour, dt, jst)) return; pn = 0.0; }
      if(pn > 0.0) { PrintFormat("[GER40] 直前レッグ%+.2f%% > 0 なので見送り", pn); g_deEntryDay = today; return; }
      PrintFormat("[GER40] 直前レッグ%+.2f%% → 建てる", pn);
      double lots = LotDe();
      if(!CanTrade()) { PrintFormat("[GER40][デモ以外] 買いシグナル lot=%.1f（発注せず）", lots); g_deEntryDay = today; return; }
      trade.SetExpertMagicNumber(InpDeMagic);
      if(trade.Buy(lots, InpDeSymbol, 0, 0, 0, "eunight")) PrintFormat("[GER40] 買い lot=%.1f(残高%.0f円) @%.2f spread=%dpt", lots, AccountInfoDouble(ACCOUNT_BALANCE), trade.ResultPrice(), spread);
      else PrintFormat("[GER40] 買い失敗 ret=%d %s", trade.ResultRetcode(), trade.ResultRetcodeDescription());
      g_deEntryDay = today;
   }
}

int OnInit()
{
   if(AnotherInstanceRunning()) { Print("XMCombo は別チャートで稼働中なので起動しません"); ExpertRemove(); return INIT_FAILED; }
   trade.SetDeviationInPoints(30);
   g_fixAM.Setup("金", InpGoldHourLon, InpGoldMinLon, InpGoldHoldMin, InpGoldMagic, InpGoldGateThr, InpGoldMonMult, InpGoldFriMult, InpGoldVolOrThr);
   g_fixPM.Setup("金PM", InpGoldPmHourLon, InpGoldPmMinLon, InpGoldPmHoldMin, InpGoldPmMagic, InpGoldPmGateThr, 1.0, 1.0, InpGoldPmVolOrThr, InpGoldPmUpSkipPct);
   if((InpGoldOn || InpGoldPmOn || InpGdOn) && !SymbolSelect(InpGoldSymbol, true)) { Print("銘柄が見つからない: ", InpGoldSymbol); return INIT_FAILED; }
   if(InpJpOn && !SymbolSelect(InpJpSymbol, true)) { Print("銘柄が見つからない: ", InpJpSymbol); return INIT_FAILED; }
   if(InpUsOn && !SymbolSelect(InpUsSymbol, true)) { Print("銘柄が見つからない: ", InpUsSymbol); return INIT_FAILED; }
   if(InpDeOn && !SymbolSelect(InpDeSymbol, true)) { Print("銘柄が見つからない: ", InpDeSymbol); return INIT_FAILED; }
   if(InpDsMode > 0 && (!SymbolSelect(InpGoldSymbol, true) || !SymbolSelect(InpDsEurUsd, true) || !SymbolSelect(InpDsUsdChf, true) || !SymbolSelect(InpDsUsdJpy, true)))
   { Print("[ドル衝撃] 銘柄が見つからない → ドル衝撃だけ止める(他は動かす)"); }
   if(!CanTrade()) Print("⚠ デモ口座ではないので発注しません(InpDemoOnly=true)");
   PrintFormat("XMCombo 起動: 残高%.0f円 全停止ライン%.0f円 | 金再開買い mode=%d(2=実弾は残高%.0f以上) | 金%s lot=%.2f(%.0f円ごと0.01・上限%.2f) SL$%.1f 売London%02d:%02d→%d分 | 日経%s lot=%.1f(%.0f円ごと1.0・上限%.1f) 買%02d:00JST→売%02d:00 週末%s 前夜フィルタ%s(月曜無条件%s) 追加%02d時≤%.2f%%x%.1f | US%s lot=%.1f(%.0f円ごと0.1・上限%.1f) | GER40%s lot=%.1f(%.0f円ごと0.1・上限%.1f) 買%02d:00JST→売%02d:00 火〜金 直前レッグ≤0 | 金昼%s lot=%.2f(%.0f円ごと0.01) 買%02d→売%02dJST | UK-DST=%s",
               AccountInfoDouble(ACCOUNT_BALANCE), InpStopBelowBalance, InpGxMode, InpGxMinBalance, InpGoldOn ? "on" : "off", LotGold(), InpGoldJpyPer001, InpGoldLotMax, InpGoldStopUsd, InpGoldHourLon, InpGoldMinLon, InpGoldHoldMin,
               InpJpOn ? "on" : "off", LotJp(), InpJpJpyPerLot, InpJpLotMax, InpJpEntryHour, InpJpExitHour, InpJpHoldWeekend ? "on" : "off", InpJpPrevNightFilter ? "on" : "off", InpJpMondayFree ? "on" : "off", InpJpAddHour, InpJpAddPct, InpJpAddMult, InpUsOn ? "on" : "off", LotUs(), InpUsJpyPer01, InpUsLotMax, InpDeOn ? "on" : "off", LotDe(), InpDeJpyPer01, InpDeLotMax, InpDeEntryHour, InpDeExitHour, InpGdOn ? "on" : "off", LotGd(), InpGdJpyPer001, InpGdEntryHour, InpGdExitHour, UkDst(TimeGMT()) ? "夏" : "冬");
   PrintFormat("[金PM] %s 売London%02d:%02d→%d分 lot=%.2f(AMと同サイズ/同ゲートN%d閾%.2f/同SL) magic=%I64d", InpGoldPmOn ? "on" : "off", InpGoldPmHourLon, InpGoldPmMinLon, InpGoldPmHoldMin, LotGold(), InpGoldGateN, InpGoldPmGateThr, InpGoldPmMagic);
   PrintFormat("[GER40] 月曜夜(JST火曜)の判定 = %s", InpDePrevWeekend ? "週末(土01:00→月16:00JST)の動き" : "木曜夜のレッグ(従来)");
   PrintFormat("[大台] %s 刻み$%.0f 上抜け+$%.1f 触れてから%d分以内 保有%d分 SL$%.1f lot=%.2f 時間帯サーバー%02d:%02d〜%02d:%02d magic=%I64d",
               InpRnOn ? "on" : "off", InpRnStep, InpRnDelta, InpRnArmMin, InpRnHoldMin, InpRnStopUsd, InpRnLot,
               InpRnStartSrvMin / 60, InpRnStartSrvMin % 60, InpRnEndSrvMin / 60, InpRnEndSrvMin % 60, InpRnMagic);
   if(InpDsMode > 0)
   {
      datetime mm = iTime(InpGoldSymbol, PERIOD_M1, 1);
      int nE = 0, nC = 0, nJ = 0;
      double sE = DsSigma(InpDsEurUsd, mm, nE), sC = DsSigma(InpDsUsdChf, mm, nC), sJ = DsSigma(InpDsUsdJpy, mm, nJ);
      PrintFormat("[ドル衝撃] mode=%d(%s) %s lot=%.2f |zu|≥%.1f 変化%d分 保有%d分 塊%d分 SL$%.0f 時間帯サーバー%02d:%02d〜%02d:%02d(撃つのは〜%02d:%02d) magic=%I64d | 15分σ(20日) EUR=%.6f(n%d) CHF=%.6f(n%d) JPY=%.6f(n%d)",
                  InpDsMode, InpDsMode == 2 ? "実弾" : "紙", InpDsSide == 1 ? "買いだけ" : (InpDsSide == -1 ? "売りだけ" : "両方"), InpDsLot, InpDsK, InpDsLookMin, InpDsHoldMin, InpDsDeclMin, InpDsStopUsd,
                  InpDsValidStartMin / 60, InpDsValidStartMin % 60, InpDsValidEndMin / 60, InpDsValidEndMin % 60, InpDsTradeEndMin / 60, InpDsTradeEndMin % 60, InpDsMagic,
                  sE, nE, sC, nC, sJ, nJ);
      DsSelfTest();
   }
   else Print("[ドル衝撃] off");
   PrintFormat("[金ボラ併用ゲート] AM>%.0f%% PM>%.0f%% (0=無効) 今の20日実現ボラ=%.1f%%", InpGoldVolOrThr, InpGoldPmVolOrThr, RealizedVol20(InpGoldSymbol));
   PrintFormat("[金PM上げ日スキップ] 前日終値比 > +%.2f%% なら撃たない (0=無効)", InpGoldPmUpSkipPct);
   PrintFormat("[ラダー設定] ≥%.0f円×%.2f / ≥%.0f円×%.2f / ≥%.0f円×%.2f → 今の残高%.0f円は×%.2f", InpScale1Bal, InpScale1, InpScale2Bal, InpScale2, InpScale3Bal, InpScale3, AccountInfoDouble(ACCOUNT_BALANCE), ScaleK());
   WarmSeries();
   EventSetTimer(5);
   return INIT_SUCCEEDED;
}
void OnDeinit(const int reason) { EventKillTimer(); }

//--- 英国(イングランド)の銀行休業日 2026-2031。LBMAのオークションが開催されない日
string g_ukHol[] = {"2026.01.01","2026.04.03","2026.04.06","2026.05.04","2026.05.25","2026.08.31","2026.12.25","2026.12.28","2027.01.01","2027.03.26","2027.03.29","2027.05.03","2027.05.31","2027.08.30","2027.12.27","2027.12.28","2028.01.03","2028.04.14","2028.04.17","2028.05.01","2028.05.29","2028.08.28","2028.12.25","2028.12.26","2029.01.01","2029.03.30","2029.04.02","2029.05.07","2029.05.28","2029.08.27","2029.12.25","2029.12.26","2030.01.01","2030.04.19","2030.04.22","2030.05.06","2030.05.27","2030.08.26","2030.12.25","2030.12.26","2031.01.01","2031.04.11","2031.04.14","2031.05.05","2031.05.26","2031.08.25","2031.12.25","2031.12.26"};
bool IsUkHoliday(datetime lonDay)
{
   string d = TimeToString(lonDay, TIME_DATE);
   for(int k = 0; k < ArraySize(g_ukHol); k++) if(g_ukHol[k] == d) return true;
   return false;
}
//--- 20日実現ボラ(年率%)。確定したD1終値20本の対数リターン標準偏差×sqrt(252)。当日の未確定足(shift0)は使わない(BTのshift(1)と同じ)。取れなければ-1
double RealizedVol20(string sym)
{
   double c[]; if(CopyClose(sym, PERIOD_D1, 1, 21, c) < 21) return -1.0;
   double r[20], s = 0; for(int i = 0; i < 20; i++) { if(c[i] <= 0 || c[i + 1] <= 0) return -1.0; r[i] = MathLog(c[i + 1] / c[i]); s += r[i]; }
   double m = s / 20, v = 0; for(int i = 0; i < 20; i++) v += (r[i] - m) * (r[i] - m);
   return MathSqrt(v / 19) * MathSqrt(252.0) * 100.0;
}
//--- 値決め直前ショートのレッグ。AM(10:19→10:32)とPM(14:53→15:03)をロンドン時刻で同じ仕組みで動かす。ゲート履歴・建玉状態はレッグごとに独立
//    窓の紙の結果($/oz・SL・コストInpGoldCostUsd=実勢$0.593): London day の 開始始値 - 終了始値
class CFixLeg
{
public:
   string   tag; int hourLon, minLon, holdMin; long magic; double gateThr; double monMult; double friMult; double volThr; double upSkip;
   double   hist[]; datetime histDay[]; datetime recDay; bool init; int initTries;
   datetime entryDay, entryTime;
   void Setup(string t, int h, int m, int hold, long mg, double gthr, double mmult = 1.0, double fmult = 1.0, double vthr = 0.0, double upsk = 0.0)
   { upSkip = upsk; tag = t; hourLon = h; minLon = m; holdMin = hold; magic = mg; gateThr = gthr; monMult = mmult; friMult = fmult; volThr = vthr; recDay = 0; init = false; initTries = 0; entryDay = 0; entryTime = 0; ArrayResize(hist, 0); ArrayResize(histDay, 0); }
   //--- ボラ併用: 20日実現ボラが閾値超なら P&Lゲートが休止でも撃つ(閾値0=無効)
   bool VolOpen() { if(volThr <= 0) return false; double v = RealizedVol20(InpGoldSymbol); return v > volThr; }
   bool Window(datetime lonDay, double &res)
   {
      datetime srvOff = TimeTradeServer() - TimeGMT();
      datetime gmt0 = lonDay + (hourLon * 60 + minLon) * 60 - (UkDst(lonDay + 12 * 3600) ? 3600 : 0);
      datetime t1 = gmt0 + srvOff, t2 = t1 + holdMin * 60;
      int b1 = iBarShift(InpGoldSymbol, PERIOD_M1, t1, true), b2 = iBarShift(InpGoldSymbol, PERIOD_M1, t2, true);
      if(b1 < 0 || b2 < 0 || b1 <= b2) return false;
      double o1 = iOpen(InpGoldSymbol, PERIOD_M1, b1), o2 = iOpen(InpGoldSymbol, PERIOD_M1, b2);
      if(o1 <= 0 || o2 <= 0) return false;
      double hi = 0; for(int b = b1; b > b2; b--) hi = MathMax(hi, iHigh(InpGoldSymbol, PERIOD_M1, b));
      res = (InpGoldStopUsd > 0 && hi >= o1 + InpGoldStopUsd) ? -InpGoldStopUsd - InpGoldCostUsd : (o1 - o2) - InpGoldCostUsd;
      return true;
   }
   void GateAppend(datetime lonDay, double r)
   { int n = ArraySize(hist); ArrayResize(hist, n + 1); ArrayResize(histDay, n + 1); hist[n] = r; histDay[n] = lonDay; }
   void GateInit()
   {
      if(init || InpGoldGateN <= 0) return;
      ArrayResize(hist, 0); ArrayResize(histDay, 0);
      datetime today = DayOf(NowLondon()); int got = 0;
      for(int d = 1; d <= 120 && got < InpGoldGateN + 10; d++)
      {
         datetime day = today - d * 86400; MqlDateTime dt; TimeToStruct(day, dt);
         if(dt.day_of_week == 0 || dt.day_of_week == 6) continue;
         double r; if(!(InpSkipUkHoliday && IsUkHoliday(day)) && Window(day, r)) { GateAppend(day, r); got++; }
      }
      int n = ArraySize(hist);   // 古い→新しい順
      for(int i = 0; i < n / 2; i++) { double t = hist[i]; hist[i] = hist[n - 1 - i]; hist[n - 1 - i] = t; datetime td = histDay[i]; histDay[i] = histDay[n - 1 - i]; histDay[n - 1 - i] = td; }
      initTries++;
      if(n < InpGoldGateN && initTries < 60) return;          // M1履歴の読込待ち(5秒×60)
      init = true;
      PrintFormat("[%sゲート] 履歴%d本を復元 直近%d回平均=%+.3f$/oz (閾値%.2f・%s)", tag, n, InpGoldGateN, GateMean(), gateThr, GateOpen() ? "稼働" : "休止");
   }
   double GateMean()
   {
      int n = ArraySize(hist); if(n == 0) return 0; int k = MathMin(n, InpGoldGateN); double s = 0;
      for(int i = n - k; i < n; i++) s += hist[i]; return s / k;
   }
   bool GateOpen() { if(InpGoldGateN <= 0) return true; if(ArraySize(hist) < InpGoldGateN) return true; return GateMean() > gateThr; }
   void GateRecord()
   {
      if(InpGoldGateN <= 0) return;
      datetime lon = NowLondon(); MqlDateTime dt; TimeToStruct(lon, dt); datetime today = DayOf(lon);
      int nowMin = dt.hour * 60 + dt.min, doneMin = hourLon * 60 + minLon + holdMin + 2;
      if(dt.day_of_week < 1 || dt.day_of_week > 5 || nowMin < doneMin || recDay == today) return;
      int n = ArraySize(histDay); if(n > 0 && histDay[n - 1] == today) { recDay = today; return; }
      datetime from = (n > 0) ? histDay[n - 1] + 86400 : today;
      if(today - from > 30 * 86400) from = today - 30 * 86400;
      for(datetime day = from; day < today; day += 86400)         // 抜けた営業日を補完
      {
         MqlDateTime dd; TimeToStruct(day, dd); if(dd.day_of_week == 0 || dd.day_of_week == 6) continue;
         double rb; if(!(InpSkipUkHoliday && IsUkHoliday(day)) && Window(day, rb)) GateAppend(day, rb);
      }
      double r; if(!Window(today, r)) return;
      if(!(InpSkipUkHoliday && IsUkHoliday(today))) GateAppend(today, r); recDay = today;
      PrintFormat("[%sゲート] 今日の窓%+.2f$/oz → 直近%d回平均%+.3f (%s)", tag, r, InpGoldGateN, GateMean(), GateOpen() ? "稼働" : "休止");
   }
   void Tick()
   {
      datetime lon = NowLondon(); MqlDateTime dt; TimeToStruct(lon, dt); datetime today = DayOf(lon);
      int nowMin = dt.hour * 60 + dt.min, entMin = hourLon * 60 + minLon;
      if(HasPos(InpGoldSymbol, magic) && entryTime == 0)
      {  // 再起動後に建玉が残っていた: 建て時刻をポジションから復元(サーバー時刻→ロンドン)
         for(int i = PositionsTotal() - 1; i >= 0; i--)
         {
            ulong tk = PositionGetTicket(i);
            if(tk > 0 && PositionSelectByTicket(tk) && PositionGetString(POSITION_SYMBOL) == InpGoldSymbol && PositionGetInteger(POSITION_MAGIC) == magic)
            { entryTime = (datetime)PositionGetInteger(POSITION_TIME) - (TimeTradeServer() - TimeGMT()) + (UkDst(TimeGMT()) ? 3600 : 0); PrintFormat("[%s] 再起動後の建玉 #%I64u を検出 建て時刻(London)=%s", tag, tk, TimeToString(entryTime, TIME_DATE | TIME_MINUTES)); break; }
         }
      }
      if(HasPos(InpGoldSymbol, magic) && entryTime > 0 && lon >= entryTime + holdMin * 60)
      { if(CanTrade()) CloseAll(InpGoldSymbol, magic, tag); if(!HasPos(InpGoldSymbol, magic)) entryTime = 0; else PrintFormat("[%s] 決済が残っている → 5秒後に再試行", tag); return; }
      if(dt.day_of_week >= 1 && dt.day_of_week <= 5 && nowMin >= entMin && nowMin < entMin + 2 && !HasPos(InpGoldSymbol, magic) && entryDay != today)
      {
         if(Halted()) { entryDay = today; return; }
         if(!GateOpen())
         {
            if(VolOpen()) PrintFormat("[%s] P&Lゲートは休止(直近%d回平均%+.3f$/oz ≤ %.2f)だが 20日ボラ%.1f%% > %.0f%% → ボラ併用で撃つ", tag, InpGoldGateN, GateMean(), gateThr, RealizedVol20(InpGoldSymbol), volThr);
            else { PrintFormat("[%s] ゲート休止: 直近%d回平均%+.3f$/oz ≤ %.2f (20日ボラ%.1f%%) → 撃たない(紙で計測は継続)", tag, InpGoldGateN, GateMean(), gateThr, RealizedVol20(InpGoldSymbol)); entryDay = today; return; }
         }
         int spread = (int)SymbolInfoInteger(InpGoldSymbol, SYMBOL_SPREAD);
         entryDay = today;
         if(spread > InpGoldMaxSpread) { PrintFormat("[%s] スプレッド%dpt > %d 見送り", tag, spread, InpGoldMaxSpread); return; }
         if(InpSkipUkHoliday && IsUkHoliday(today))
         { PrintFormat("[%s] 英国銀行休業日=LBMAオークション無し → 撃たない", tag); entryDay = today; return; }
         if(upSkip > 0)
         {
            double pc = iClose(InpGoldSymbol, PERIOD_D1, 1), bid = SymbolInfoDouble(InpGoldSymbol, SYMBOL_BID);
            if(pc > 0 && bid > 0 && (bid / pc - 1.0) * 100.0 > upSkip)
            { PrintFormat("[%s] 前日終値%.2f→今%.2f (%+.2f%% > +%.2f%%) の上げ日 → 撃たない", tag, pc, bid, (bid / pc - 1.0) * 100.0, upSkip); return; }
         }
         double lots = LotGold();
         {
            MqlDateTime df; TimeToStruct(NowLondon(), df);
            if(df.day_of_week == 5 && friMult != 1.0)
            {
               double vstep = SymbolInfoDouble(InpGoldSymbol, SYMBOL_VOLUME_STEP);
               double vmin  = SymbolInfoDouble(InpGoldSymbol, SYMBOL_VOLUME_MIN);
               double f = NormalizeDouble(MathRound(lots * friMult / vstep + 1e-9) * vstep, 2);
               f = MathMax(vmin, f);
               PrintFormat("[%s] 金曜なのでロットx%.2f: %.2f → %.2f", tag, friMult, lots, f);
               lots = f;
            }
         }
         if(monMult > 1.0)
         {
            MqlDateTime dl; TimeToStruct(NowLondon(), dl);
            if(dl.day_of_week == 1)
            {
               double vstep = SymbolInfoDouble(InpGoldSymbol, SYMBOL_VOLUME_STEP);
               double big = NormalizeDouble(MathFloor(lots * monMult / vstep + 1e-9) * vstep, 2);
               big = MathMin(big, InpGoldLotMax);
               PrintFormat("[%s] 月曜なのでロット×%.1f: %.2f → %.2f", tag, monMult, lots, big);
               lots = big;
            }
         }
         if(!CanTrade()) { PrintFormat("[%s][デモ以外] 売りシグナル lot=%.2f（発注せず）", tag, lots); return; }
         double slpx = InpGoldStopUsd > 0 ? NormalizeDouble(SymbolInfoDouble(InpGoldSymbol, SYMBOL_ASK) + InpGoldStopUsd, (int)SymbolInfoInteger(InpGoldSymbol, SYMBOL_DIGITS)) : 0.0;
         trade.SetExpertMagicNumber(magic);
         if(trade.Sell(lots, InpGoldSymbol, 0, slpx, 0, tag == "金" ? "fix" : "fixpm"))
         { entryTime = lon; PrintFormat("[%s] 売り lot=%.2f(残高%.0f円) @%.2f SL=%.2f spread=%dpt London %02d:%02d", tag, lots, AccountInfoDouble(ACCOUNT_BALANCE), trade.ResultPrice(), slpx, spread, dt.hour, dt.min); }
         else PrintFormat("[%s] 売り失敗 ret=%d %s", tag, trade.ResultRetcode(), trade.ResultRetcodeDescription());
      }
   }
};
CFixLeg g_fixAM, g_fixPM;

void IdxTick(string sym, long magic, double lots, int maxSpread, string tag, datetime &entryDay, datetime &exitDay, int eh, int xh)
{
   datetime jst = NowJST(); MqlDateTime dt; TimeToStruct(jst, dt); datetime today = DayOf(jst);
   if(dt.hour >= xh && dt.hour < eh && HasPos(sym, magic) && exitDay != today)
   { if(CanTrade()) CloseAll(sym, magic, tag); if(!HasPos(sym, magic)) exitDay = today; else PrintFormat("[%s] 決済が残っている → 5秒後に再試行", tag); return; }
   bool okDay = (dt.day_of_week >= 1 && dt.day_of_week <= 4) || (InpJpHoldWeekend && dt.day_of_week == 5);
   if(dt.hour >= eh && okDay && !HasPos(sym, magic) && entryDay != today)
   {
      if(dt.hour >= eh + 3) { PrintFormat("[%s] %02d時以降なので今日は建てない", tag, dt.hour); entryDay = today; return; }
      if(Halted()) { entryDay = today; return; }
      int spread = (int)SymbolInfoInteger(sym, SYMBOL_SPREAD);
      if(spread > maxSpread) { PrintFormat("[%s] スプレッド%dpt > %d 見送り(再試行)", tag, spread, maxSpread); return; }
      if(InpJpPrevNightFilter && !(InpJpMondayFree && dt.day_of_week == 1))
      {
         double pn = PrevNightPct(sym, eh, xh);
         if(pn == NA_PCT) { if(NaRetry(tag, eh, dt, jst)) return; pn = 0.0; }
         if(pn > InpJpPrevNightMax) { PrintFormat("[%s] 前夜%+.2f%% > %.2f%% なので今夜は見送り", tag, pn, InpJpPrevNightMax); entryDay = today; return; }
         PrintFormat("[%s] 前夜%+.2f%% → 建てる", tag, pn);
      }
      else if(InpJpPrevNightFilter) PrintFormat("[%s] 月曜はフィルタ無しで建てる", tag);
      if(!CanTrade()) { PrintFormat("[%s][デモ以外] 買いシグナル lot=%.1f（発注せず）", tag, lots); entryDay = today; return; }
      trade.SetExpertMagicNumber(magic);
      if(trade.Buy(lots, sym, 0, 0, 0, "night")) PrintFormat("[%s] 買い lot=%.1f(残高%.0f円) @%.2f spread=%dpt", tag, lots, AccountInfoDouble(ACCOUNT_BALANCE), trade.ResultPrice(), spread);
      else PrintFormat("[%s] 買い失敗 ret=%d %s", tag, trade.ResultRetcode(), trade.ResultRetcodeDescription());
      entryDay = today;
   }
}
void JpAddTick()
{
   if(InpJpAddHour <= 0) return;
   datetime jst = NowJST(); MqlDateTime dt; TimeToStruct(jst, dt); datetime today = DayOf(jst);
   if(dt.hour != InpJpAddHour || g_jpAddDay == today || Halted()) return;
   if(!HasPos(InpJpSymbol, InpJpMagic)) { g_jpAddDay = today; return; }
   double lots = 0, entry = 0; int cnt = 0;
   for(int i = PositionsTotal() - 1; i >= 0; i--)
   {
      ulong tk = PositionGetTicket(i);
      if(tk > 0 && PositionSelectByTicket(tk) && PositionGetString(POSITION_SYMBOL) == InpJpSymbol && PositionGetInteger(POSITION_MAGIC) == InpJpMagic)
      { lots += PositionGetDouble(POSITION_VOLUME); entry = PositionGetDouble(POSITION_PRICE_OPEN); cnt++; }
   }
   if(cnt != 1 || entry <= 0) { g_jpAddDay = today; return; }              // 既に追加済み等
   double bid = SymbolInfoDouble(InpJpSymbol, SYMBOL_BID); if(bid <= 0) return;
   double pl = (bid / entry - 1.0) * 100.0;
   if(pl > InpJpAddPct) { PrintFormat("[日経追加] %02d時 建値比%+.2f%% > %.2f%% 追加なし", dt.hour, pl, InpJpAddPct); g_jpAddDay = today; return; }
   int spread = (int)SymbolInfoInteger(InpJpSymbol, SYMBOL_SPREAD);
   if(spread > InpJpMaxSpread) { PrintFormat("[日経追加] スプレッド%dpt > %d 見送り(再試行)", spread, InpJpMaxSpread); return; }
   double add = NormalizeDouble(MathFloor(lots * InpJpAddMult / 0.1 + 1e-9) * 0.1, 2);
   if(add < SymbolInfoDouble(InpJpSymbol, SYMBOL_VOLUME_MIN)) { g_jpAddDay = today; return; }
   if(!CanTrade()) { PrintFormat("[日経追加][デモ以外] 建値比%+.2f%% 追加シグナル lot=%.1f（発注せず）", pl, add); g_jpAddDay = today; return; }
   trade.SetExpertMagicNumber(InpJpMagic);
   if(trade.Buy(add, InpJpSymbol, 0, 0, 0, "night-add")) PrintFormat("[日経追加] 建値比%+.2f%% → 追加買い lot=%.1f @%.1f spread=%dpt", pl, add, trade.ResultPrice(), spread);
   else PrintFormat("[日経追加] 失敗 ret=%d %s", trade.ResultRetcode(), trade.ResultRetcodeDescription());
   g_jpAddDay = today;
}
void JpTick() { IdxTick(InpJpSymbol, InpJpMagic, LotJp(), InpJpMaxSpread, "日経", g_jpEntryDay, g_jpExitDay, InpJpEntryHour, InpJpExitHour); JpAddTick(); }
void UsTick() { string ustag = (StringFind(InpUsSymbol, "US100") >= 0) ? "US100" : "US500"; IdxTick(InpUsSymbol, InpUsMagic, LotUs(), InpUsMaxSpread, ustag, g_usEntryDay, g_usExitDay, InpUsEntryHour, InpUsExitHour); }

double LotGx()
{
   double lot = MathFloor(AccountInfoDouble(ACCOUNT_BALANCE) / InpGxJpyPer001) * 0.01;
   double vmin = SymbolInfoDouble(InpGoldSymbol, SYMBOL_VOLUME_MIN), vstep = SymbolInfoDouble(InpGoldSymbol, SYMBOL_VOLUME_STEP);
   lot = MathMax(vmin, MathMin(InpGxLotMax, lot)); return NormalizeDouble(MathFloor(lot / vstep) * vstep, 2);
}
void GxRecord(string line)
{
   int h = FileOpen("XMCombo_globex_paper.csv", FILE_READ | FILE_WRITE | FILE_TXT | FILE_ANSI | FILE_SHARE_READ);
   if(h == INVALID_HANDLE) return;
   FileSeek(h, 0, SEEK_END); FileWriteString(h, line + "\r\n"); FileClose(h);
}
void GxTick()
{
   if(InpGxMode == 0) return;
   datetime srv = TimeTradeServer(); MqlDateTime dt; TimeToStruct(srv, dt); datetime today = DayOf(srv);
   bool live = (InpGxMode == 2 && AccountInfoDouble(ACCOUNT_BALANCE) >= InpGxMinBalance && CanTrade() && !Halted());
   int nowMin = dt.hour * 60 + dt.min, entMin = InpGxEntryHourSrv * 60 + InpGxEntryMinSrv, exitMin = InpGxExitHourSrv * 60;
   //--- 決済
   if(nowMin >= exitMin && nowMin < exitMin + 30)
   {
      if(live && HasPos(InpGoldSymbol, InpGxMagic)) CloseAll(InpGoldSymbol, InpGxMagic, "金再開");
      if(g_gxPaperOpen)
      {
         double bid = SymbolInfoDouble(InpGoldSymbol, SYMBOL_BID); double pnl = bid - g_gxPaperEntry;
         PrintFormat("[金再開][紙] 決済 @%.2f 建%.2f 差%+.2f$/oz (%+.3f%%)", bid, g_gxPaperEntry, pnl, pnl / g_gxPaperEntry * 100);
         GxRecord(StringFormat("%s,%s,%.2f,%.2f,%.2f,%.4f", TimeToString(g_gxPaperTime, TIME_DATE | TIME_MINUTES), TimeToString(srv, TIME_DATE | TIME_MINUTES), g_gxPaperEntry, bid, pnl, pnl / g_gxPaperEntry * 100));
         g_gxPaperOpen = false;
      }
      return;
   }
   //--- 建て: 01:05〜01:07（スプレッドが広ければ最大60秒待つ）
   if(dt.day_of_week >= 1 && dt.day_of_week <= 5 && nowMin >= entMin && nowMin < entMin + 3 && g_gxDay != today)
   {
      int spread = (int)SymbolInfoInteger(InpGoldSymbol, SYMBOL_SPREAD);
      double ask = SymbolInfoDouble(InpGoldSymbol, SYMBOL_ASK);
      if(ask <= 0) return;                                            // ティック未着
      if(spread > InpGxMaxSpread)
      {
         if(g_gxWaitStart == 0) g_gxWaitStart = srv;
         if(srv - g_gxWaitStart < 60) return;
         PrintFormat("[金再開] スプレッド%dpt > %d が60秒続いたので見送り", spread, InpGxMaxSpread); g_gxDay = today; g_gxWaitStart = 0; return;
      }
      g_gxDay = today; g_gxWaitStart = 0;
      if(live)
      {
         trade.SetExpertMagicNumber(InpGxMagic);
         if(trade.Buy(LotGx(), InpGoldSymbol, 0, 0, 0, "globex")) PrintFormat("[金再開] 買い lot=%.2f @%.2f spread=%dpt", LotGx(), trade.ResultPrice(), spread);
         else PrintFormat("[金再開] 買い失敗 ret=%d %s", trade.ResultRetcode(), trade.ResultRetcodeDescription());
      }
      g_gxPaperEntry = ask; g_gxPaperTime = srv; g_gxPaperOpen = true;
      PrintFormat("[金再開][%s] 建て @%.2f spread=%dpt %s", live ? "実弾" : "紙", ask, spread, TimeToString(srv, TIME_DATE | TIME_MINUTES | TIME_SECONDS));
   }
}

datetime g_hbLast = 0;
void Heartbeat()
{
   datetime now = TimeGMT(); if(now - g_hbLast < 30) return; g_hbLast = now;
   int h = FileOpen("XMCombo_heartbeat.txt", FILE_WRITE | FILE_TXT | FILE_ANSI);
   if(h == INVALID_HANDLE) return;
   FileWriteString(h, StringFormat("%s balance=%.0f equity=%.0f positions=%d connected=%d", TimeToString(now, TIME_DATE | TIME_MINUTES | TIME_SECONDS), AccountInfoDouble(ACCOUNT_BALANCE), AccountInfoDouble(ACCOUNT_EQUITY), PositionsTotal(), (int)TerminalInfoInteger(TERMINAL_CONNECTED)));
   FileClose(h);
}
//--- D) 金 大台ブレイク（入力は InpRn*・既定off）
datetime g_rnDay = 0;            // サーバー日(0時)
double   g_rnDone[];             // その日に触れた大台(1日1回)
datetime g_rnArmTime = 0;        // 大台に触れた足の時刻(サーバー)。再起動後は0→逆指値の発注時刻で代用
bool RnDone(double L) { for(int i = 0; i < ArraySize(g_rnDone); i++) if(MathAbs(g_rnDone[i] - L) < 1e-6) return true; return false; }
void RnMarkDone(double L) { int n = ArraySize(g_rnDone); ArrayResize(g_rnDone, n + 1); g_rnDone[n] = L; }
ulong RnPendingTicket()
{
   for(int i = OrdersTotal() - 1; i >= 0; i--)
   {
      ulong tk = OrderGetTicket(i);
      if(tk > 0 && OrderGetString(ORDER_SYMBOL) == InpGoldSymbol && OrderGetInteger(ORDER_MAGIC) == InpRnMagic) return tk;
   }
   return 0;
}
datetime RnPosTime()
{
   for(int i = PositionsTotal() - 1; i >= 0; i--)
   {
      ulong tk = PositionGetTicket(i);
      if(tk > 0 && PositionSelectByTicket(tk) && PositionGetString(POSITION_SYMBOL) == InpGoldSymbol && PositionGetInteger(POSITION_MAGIC) == InpRnMagic)
         return (datetime)PositionGetInteger(POSITION_TIME);
   }
   return 0;
}
//--- 当日(サーバー日)の、足 b より古い足の高値の最大(足 b は含まない)。当日の足が無ければ -1、未ロードなら -2
double RnDayHighBefore(int b, datetime sday)
{
   double h = -1.0;
   for(int k = b + 1; k < 1500; k++)
   {
      datetime t = iTime(InpGoldSymbol, PERIOD_M1, k);
      if(t <= 0) return -2.0;
      if(t < sday) break;
      double x = iHigh(InpGoldSymbol, PERIOD_M1, k);
      if(x > h) h = x;
   }
   return h;
}
void RnTick()
{
   string sym = InpGoldSymbol;
   datetime srv = TimeTradeServer();
   datetime sday = DayOf(srv);
   int smin = (int)((srv - sday) / 60);
   // 1) 建玉: 保有分数で時間決済(損切りはブローカー側のSL)
   datetime pt = RnPosTime();
   if(pt > 0)
   {
      if(srv >= pt + InpRnHoldMin * 60)
      {
         if(CanTrade()) CloseAll(sym, InpRnMagic, "大台");
         if(RnPosTime() > 0) Print("[大台] 決済が残っている → 5秒後に再試行");
      }
      return;
   }
   // 2) 待機中の逆指値: 触れてから InpRnArmMin 分・時間帯の終わり・日替わりで取り消し
   ulong ot = RnPendingTicket();
   if(ot > 0 && OrderSelect(ot))
   {
      datetime setup = (datetime)OrderGetInteger(ORDER_TIME_SETUP);
      datetime base = (g_rnArmTime > 0) ? g_rnArmTime : setup;
      if(srv > base + InpRnArmMin * 60 || smin > InpRnEndSrvMin || sday != DayOf(setup))
      {
         trade.SetExpertMagicNumber(InpRnMagic);
         if(trade.OrderDelete(ot)) PrintFormat("[大台] 逆指値を取り消し(期限/時間帯外) #%I64u", ot);
         else PrintFormat("[大台] 逆指値の取り消し失敗 #%I64u ret=%d", ot, trade.ResultRetcode());
         g_rnArmTime = 0;
      }
      return;
   }
   // 3) 日替わりリセット・時間帯
   if(sday != g_rnDay) { g_rnDay = sday; ArrayResize(g_rnDone, 0); g_rnArmTime = 0; }
   if(smin < InpRnStartSrvMin || smin > InpRnEndSrvMin) return;
   if(Halted()) return;
   // 4) 触れた判定(BTと同じ): 直前の足の終値 < L ≤ その足の高値、かつ当日のそれより前の高値 < L。
   //    足0(今の足)に加えて足1も見る＝5秒の見回りの隙間で分をまたいだ時も見逃さない
   for(int b = 1; b >= 0; b--)
   {
      datetime bt = iTime(sym, PERIOD_M1, b);
      datetime pbt = iTime(sym, PERIOD_M1, b + 1);
      if(bt <= 0 || pbt <= 0 || bt < sday || pbt < sday) continue;      // 直前の足が前日なら判定しない(BTと同じ)
      int bmin = (int)((bt - sday) / 60);
      if(bmin < InpRnStartSrvMin || bmin > InpRnEndSrvMin) continue;
      double pc = iClose(sym, PERIOD_M1, b + 1);
      double hb = iHigh(sym, PERIOD_M1, b);
      if(pc <= 0 || hb <= 0) continue;
      double L = MathFloor(pc / InpRnStep) * InpRnStep + InpRnStep;   // 直前終値の上の最初の大台
      if(hb < L || RnDone(L)) continue;
      double dh = RnDayHighBefore(b, sday);
      if(dh == -2.0) return;                                           // 足が未ロード→次の5秒で
      RnMarkDone(L);
      if(dh >= L) continue;                                            // 当日すでに触れていた大台
      if(srv > bt + InpRnArmMin * 60) continue;
      // 5) 注文: BIDが L+δ に届いた時 = ASKが L+δ+スプレッド。今すでに超えていたら成行(BTの「窓で飛び越えた時は始値」に相当)
      double bid = SymbolInfoDouble(sym, SYMBOL_BID), ask = SymbolInfoDouble(sym, SYMBOL_ASK);
      int spread = (int)SymbolInfoInteger(sym, SYMBOL_SPREAD);
      if(spread > InpRnMaxSpread) { PrintFormat("[大台] $%.0fに触れたがスプレッド%dpt > %d なので見送り", L, spread, InpRnMaxSpread); return; }
      int dg = (int)SymbolInfoInteger(sym, SYMBOL_DIGITS);
      double trig = NormalizeDouble(L + InpRnDelta + (ask - bid), dg);
      double vmin = SymbolInfoDouble(sym, SYMBOL_VOLUME_MIN), vstep = SymbolInfoDouble(sym, SYMBOL_VOLUME_STEP);
      double lot = MathMax(vmin, NormalizeDouble(MathFloor(InpRnLot / vstep + 1e-9) * vstep, 2));
      if(!CanTrade()) { PrintFormat("[大台][デモ以外] $%.0fに触れた → 買い逆指値%.2f lot=%.2f（発注せず）", L, trig, lot); return; }
      trade.SetExpertMagicNumber(InpRnMagic);
      g_rnArmTime = bt;
      if(ask >= trig)
      {
         double sl = (InpRnStopUsd > 0) ? NormalizeDouble(ask - InpRnStopUsd, dg) : 0.0;
         if(trade.Buy(lot, sym, 0, sl, 0, "rn-mkt")) PrintFormat("[大台] $%.0f 上抜け済み → 成行買い lot=%.2f @%.2f SL=%.2f", L, lot, trade.ResultPrice(), sl);
         else PrintFormat("[大台] 成行買い失敗 ret=%d %s", trade.ResultRetcode(), trade.ResultRetcodeDescription());
      }
      else
      {
         double sl = (InpRnStopUsd > 0) ? NormalizeDouble(trig - InpRnStopUsd, dg) : 0.0;
         if(trade.BuyStop(lot, trig, sym, sl, 0, ORDER_TIME_GTC, 0, "rn-stop"))
            PrintFormat("[大台] $%.0f に今日初めて下から触れた → 買い逆指値 %.2f lot=%.2f SL=%.2f（%d分で取り消し・約定後%d分で決済）", L, trig, lot, sl, InpRnArmMin, InpRnHoldMin);
         else PrintFormat("[大台] 逆指値の発注失敗 ret=%d %s", trade.ResultRetcode(), trade.ResultRetcodeDescription());
      }
      return;
   }
}

//--- E) 金 ドル衝撃（入力は InpDs*・既定off）。判定は1分ごと・直前に確定した金の足の分 m で BT と同じ式
datetime g_dsLastEval = 0;       // 判定した分(足の時刻・サーバー)
datetime g_dsLastEvent = 0;      // 最後の「発生」の分(撃たなかった発生も・15分の塊の判定)
datetime g_dsPaperEntry = 0; double g_dsPaperPx = 0.0; int g_dsPaperDir = 0;
int DsSgn(double x) { return x > 0 ? 1 : (x < 0 ? -1 : 0); }
void DsRecord(string line)
{
   int h = FileOpen("XMCombo_dollarshock.csv", FILE_READ | FILE_WRITE | FILE_TXT | FILE_ANSI | FILE_SHARE_READ);
   if(h == INVALID_HANDLE) return;
   FileSeek(h, 0, SEEK_END); FileWriteString(h, line + "\r\n"); FileClose(h);
}
//--- 足の時刻 t(分の頭・サーバー)の終値。その分に足が無ければ -1
double DsCloseAt(string sym, datetime t)
{
   int sh = iBarShift(sym, PERIOD_M1, t, true);
   if(sh < 0) return -1.0;
   return iClose(sym, PERIOD_M1, sh);
}
//--- 15分の対数変化の標準偏差: 窓=[m−Win分, m−1分] の各分のうち「その分と15分前の両方に足がある」値(母標準偏差・BTの rolling と同じ)。
//    値が InpDsMinN 未満・足が未ロードなら -1。n に値の数を返す
double DsSigma(string sym, datetime m, int &n)
{
   n = 0;
   MqlRates r[];
   datetime from = m - (datetime)(InpDsWinMin + InpDsLookMin) * 60;
   datetime to = m - 60;
   int cnt = CopyRates(sym, PERIOD_M1, from, to, r);                 // 古い順
   if(cnt <= 0) return -1.0;
   datetime lo = m - (datetime)InpDsWinMin * 60;
   double s = 0.0, ss = 0.0;
   int j = 0;
   for(int i = 0; i < cnt; i++)
   {
      datetime t = r[i].time;
      datetime tp = t - InpDsLookMin * 60;
      while(j < i && r[j].time < tp) j++;
      if(t < lo) continue;
      if(j < i && r[j].time == tp && r[j].close > 0 && r[i].close > 0)
      {
         double x = MathLog(r[i].close / r[j].close);
         s += x; ss += x * x; n++;
      }
   }
   if(n < InpDsMinN) return -1.0;
   double mu = s / n, var = ss / n - mu * mu;
   return var > 0 ? MathSqrt(var) : -1.0;
}
//--- z = 15分の対数変化 ÷ 標準偏差。使えなければ ok=false
double DsZ(string sym, datetime m, bool &ok)
{
   ok = false;
   double c = DsCloseAt(sym, m), c0 = DsCloseAt(sym, m - InpDsLookMin * 60);
   if(c <= 0 || c0 <= 0) return 0.0;
   int n = 0;
   double sd = DsSigma(sym, m, n);
   if(sd <= 0) return 0.0;
   ok = true;
   return MathLog(c / c0) / sd;
}
//--- 金の足が30分以上途切れた後の60分以内か(BTの after_gap と同じ: [m−59分, m] の金の足のどれかの直前の空白が30分超)
bool DsAfterGap(datetime m)
{
   int sm = iBarShift(InpGoldSymbol, PERIOD_M1, m, true);
   if(sm < 0) return true;
   for(int k = sm; k < sm + 70; k++)
   {
      datetime tb = iTime(InpGoldSymbol, PERIOD_M1, k);
      if(tb <= 0) return true;                                         // 足が未ロード→撃たない側に倒す
      if(tb < m - 59 * 60) break;
      datetime tp = iTime(InpGoldSymbol, PERIOD_M1, k + 1);
      if(tp <= 0) return true;
      if(tb - tp > 30 * 60) return true;
   }
   return false;
}
//--- 検証用: 過去 InpDsSelfTestMin 本の金の分足で z を計算して書く(時間帯の条件なし・Pythonの双子と数字を突き合わせる)
void DsSelfTest()
{
   if(InpDsSelfTestMin <= 0) return;
   int h = FileOpen("XMCombo_ds_selftest.csv", FILE_WRITE | FILE_TXT | FILE_ANSI);
   if(h == INVALID_HANDLE) { Print("[ドル衝撃] 自己テストのファイルを開けない"); return; }
   FileWriteString(h, "m,zE,zC,zJ,zu,yg\r\n");
   uint t0 = GetTickCount();
   int nOut = 0;
   for(int k = InpDsSelfTestMin; k >= 1; k--)
   {
      datetime m = iTime(InpGoldSymbol, PERIOD_M1, k);
      if(m <= 0) continue;
      double gc = DsCloseAt(InpGoldSymbol, m), gc0 = DsCloseAt(InpGoldSymbol, m - InpDsLookMin * 60);
      if(gc <= 0 || gc0 <= 0) continue;
      bool a, b, c;
      double zE = DsZ(InpDsEurUsd, m, a);
      if(!a) continue;
      double zC = DsZ(InpDsUsdChf, m, b);
      if(!b) continue;
      double zJ = DsZ(InpDsUsdJpy, m, c);
      if(!c) continue;
      FileWriteString(h, StringFormat("%s,%.6f,%.6f,%.6f,%.6f,%.8f\r\n", TimeToString(m, TIME_DATE | TIME_MINUTES), zE, zC, zJ, (-zE + zC + zJ) / 3.0, MathLog(gc / gc0)));
      nOut++;
   }
   FileClose(h);
   PrintFormat("[ドル衝撃] 自己テスト: 過去%d本の金の分足のうち%d分で z を計算して XMCombo_ds_selftest.csv に書いた（%.1f秒）", InpDsSelfTestMin, nOut, (GetTickCount() - t0) / 1000.0);
}
datetime DsPosTime()
{
   for(int i = PositionsTotal() - 1; i >= 0; i--)
   {
      ulong tk = PositionGetTicket(i);
      if(tk > 0 && PositionSelectByTicket(tk) && PositionGetString(POSITION_SYMBOL) == InpGoldSymbol && PositionGetInteger(POSITION_MAGIC) == InpDsMagic)
         return (datetime)PositionGetInteger(POSITION_TIME);
   }
   return 0;
}
void DsTick()
{
   string sym = InpGoldSymbol;
   datetime srv = TimeTradeServer();
   // 1) 時間決済: 建てた分の頭から InpDsHoldMin 分(BTは m+1 の始値で建てて m+1+H の始値で決済)
   if(InpDsMode == 2)
   {
      datetime pt = DsPosTime();
      if(pt > 0 && srv >= pt - (pt % 60) + InpDsHoldMin * 60)
      {
         if(CanTrade()) CloseAll(sym, InpDsMagic, "ドル衝撃");
         if(DsPosTime() > 0) Print("[ドル衝撃] 決済が残っている → 5秒後に再試行");
      }
   }
   if(InpDsMode == 1 && g_dsPaperEntry > 0 && srv >= g_dsPaperEntry + InpDsHoldMin * 60)
   {
      double x = (g_dsPaperDir > 0) ? SymbolInfoDouble(sym, SYMBOL_BID) : SymbolInfoDouble(sym, SYMBOL_ASK);
      double pnl = g_dsPaperDir * (x - g_dsPaperPx);
      PrintFormat("[ドル衝撃][紙] 決済 %s %.2f→%.2f %+.2f$/oz", g_dsPaperDir > 0 ? "買い" : "売り", g_dsPaperPx, x, pnl);
      DsRecord(StringFormat("%s,paper_exit,%d,%.2f,%.2f,%.2f", TimeToString(srv, TIME_DATE | TIME_SECONDS), g_dsPaperDir, g_dsPaperPx, x, pnl));
      g_dsPaperEntry = 0;
   }
   // 2) 直前に確定した金の足の分 m を1回だけ判定する(その分の終わりから2秒以上たってから)
   datetime m = iTime(sym, PERIOD_M1, 1);
   if(m <= 0 || m <= g_dsLastEval || srv < m + 62) return;
   g_dsLastEval = m;
   int tod = (int)((m - DayOf(m)) / 60);
   if(tod < InpDsValidStartMin || tod > InpDsValidEndMin) return;
   double gc = DsCloseAt(sym, m), gc0 = DsCloseAt(sym, m - InpDsLookMin * 60);
   if(gc <= 0 || gc0 <= 0) return;
   bool oke, okc, okj;
   double zE = DsZ(InpDsEurUsd, m, oke);
   if(!oke) return;
   double zC = DsZ(InpDsUsdChf, m, okc);
   if(!okc) return;
   double zJ = DsZ(InpDsUsdJpy, m, okj);
   if(!okj) return;
   double zu = (-zE + zC + zJ) / 3.0;
   double yg = MathLog(gc / gc0);
   bool agree = (DsSgn(-zE) == DsSgn(zC) && DsSgn(zC) == DsSgn(zJ));
   int d = -DsSgn(zu);
   if(MathAbs(zu) >= 3.0)                                             // 検証用(Pythonの双子と突き合わせる): 大きめの分は全部残す
      DsRecord(StringFormat("%s,eval,%.4f,%.4f,%.4f,%.4f,%.6f,%d", TimeToString(m, TIME_DATE | TIME_MINUTES), zE, zC, zJ, zu, yg, agree ? 1 : 0));
   if(!agree || d == 0 || MathAbs(zu) < InpDsK) return;
   // ここで「発生」(BTの valid & |zu|≥k)。15分以内に続く発生は同じ塊＝最初だけ撃つ
   bool isNew = (g_dsLastEvent == 0 || m - g_dsLastEvent > InpDsDeclMin * 60);
   g_dsLastEvent = m;
   string why = "";
   if(!isNew) why = "塊の続き";
   else if(DsSgn(yg) * d <= 0) why = "金が同じ向きに動いていない";
   else if(tod > InpDsTradeEndMin) why = "保有が23:00にかかる";
   else if(DsAfterGap(m)) why = "金の足の空白の後60分";
   else if((InpDsSide == 1 && d < 0) || (InpDsSide == -1 && d > 0)) why = (d > 0 ? "買いは使わない設定" : "売りは使わない設定");
   else if(DsPosTime() > 0 || g_dsPaperEntry > 0) why = "保有中";
   else if(Halted()) why = "全停止中";
   PrintFormat("[ドル衝撃] 発生 %s zu=%+.2f(EUR%+.2f CHF%+.2f JPY%+.2f) ドル%s 金15分%+.3f%% → %s", TimeToString(m, TIME_DATE | TIME_MINUTES), zu, zE, zC, zJ,
               d > 0 ? "安" : "高", yg * 100, why == "" ? (d > 0 ? "金を買う" : "金を売る") : ("見送り: " + why));
   DsRecord(StringFormat("%s,event,%.4f,%.4f,%.4f,%.4f,%.6f,%d,%s", TimeToString(m, TIME_DATE | TIME_MINUTES), zE, zC, zJ, zu, yg, d, why == "" ? "ENTRY" : why));
   if(why != "") return;
   int spread = (int)SymbolInfoInteger(sym, SYMBOL_SPREAD);
   if(spread > InpDsMaxSpread) { PrintFormat("[ドル衝撃] スプレッド%dpt > %d なので見送り", spread, InpDsMaxSpread); return; }
   double bid = SymbolInfoDouble(sym, SYMBOL_BID), ask = SymbolInfoDouble(sym, SYMBOL_ASK);
   if(InpDsMode == 1)
   {
      g_dsPaperEntry = m + 60; g_dsPaperDir = d; g_dsPaperPx = (d > 0) ? ask : bid;
      PrintFormat("[ドル衝撃][紙] %s %.2f（%d分後に決済）", d > 0 ? "買い" : "売り", g_dsPaperPx, InpDsHoldMin);
      DsRecord(StringFormat("%s,paper_entry,%d,%.2f", TimeToString(srv, TIME_DATE | TIME_SECONDS), d, g_dsPaperPx));
      return;
   }
   if(!CanTrade()) { PrintFormat("[ドル衝撃][デモ以外] %s（発注せず）", d > 0 ? "買い" : "売り"); return; }
   int dg = (int)SymbolInfoInteger(sym, SYMBOL_DIGITS);
   double vmin = SymbolInfoDouble(sym, SYMBOL_VOLUME_MIN), vstep = SymbolInfoDouble(sym, SYMBOL_VOLUME_STEP);
   double lot = MathMax(vmin, NormalizeDouble(MathFloor(InpDsLot / vstep + 1e-9) * vstep, 2));
   trade.SetExpertMagicNumber(InpDsMagic);
   bool ok;
   if(d > 0)
   {
      double sl = (InpDsStopUsd > 0) ? NormalizeDouble(ask - InpDsStopUsd, dg) : 0.0;
      ok = trade.Buy(lot, sym, 0, sl, 0, "ds-buy");
   }
   else
   {
      double sl = (InpDsStopUsd > 0) ? NormalizeDouble(bid + InpDsStopUsd, dg) : 0.0;
      ok = trade.Sell(lot, sym, 0, sl, 0, "ds-sell");
   }
   if(ok) PrintFormat("[ドル衝撃] %s lot=%.2f @%.2f SL$%.0f（%d分で決済）", d > 0 ? "買い" : "売り", lot, trade.ResultPrice(), InpDsStopUsd, InpDsHoldMin);
   else PrintFormat("[ドル衝撃] 発注失敗 ret=%d %s", trade.ResultRetcode(), trade.ResultRetcodeDescription());
   DsRecord(StringFormat("%s,order,%d,%.2f,%d", TimeToString(srv, TIME_DATE | TIME_SECONDS), d, ok ? trade.ResultPrice() : 0.0, ok ? 1 : 0));
}

void OnTimer()
{
   Heartbeat(); WarmSeries();
   if(InpGoldOn)   { g_fixAM.GateInit(); g_fixAM.GateRecord(); }
   if(InpGoldPmOn) { g_fixPM.GateInit(); g_fixPM.GateRecord(); }
   GxTick();
   if(InpGoldOn) g_fixAM.Tick();
   if(InpGoldPmOn) g_fixPM.Tick();
   if(InpJpOn) JpTick();
   if(InpUsOn) UsTick();
   if(InpDeOn) DeTick();
   if(InpGdOn) GdTick();
   if(InpRnOn) RnTick();
   if(InpDsMode > 0) DsTick();
}
//+------------------------------------------------------------------+
