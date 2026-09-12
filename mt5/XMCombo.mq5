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
input double InpGoldJpyPer001 = 50000;      // 0.01lotあたりの必要残高(円)
input double InpGoldLotMax    = 1.00;       // 上限ロット
input double InpGoldStopUsd   = 10.0;       // 損切り幅($/oz・0=無し)
input int    InpGoldMaxSpread = 40;         // 許容スプレッド(pt=0.01$)
input int    InpGoldHourLon   = 10;         // 売り時刻 ロンドン(時)
input int    InpGoldMinLon    = 15;         // 売り時刻 ロンドン(分)
input int    InpGoldHoldMin   = 15;         // 保有分数
input long   InpGoldMagic     = 20260908;
//--- B) 日経225 夜ドリフト
input bool   InpJpOn          = true;       // 日経夜ドリフトを動かす
input string InpJpSymbol      = "JP225Cash";
input double InpJpJpyPerLot   = 7000;       // 1.0lot(名目約6.5万円)あたりの必要残高(円)
input double InpJpLotMax      = 500.0;
input int    InpJpEntryHour   = 15;         // 買い時刻 JST
input int    InpJpExitHour    = 9;          // 手仕舞い時刻 JST
input bool   InpJpHoldWeekend = false;      // 金曜も建てて月曜朝に閉じる
input bool   InpJpPrevNightFilter = true;   // 前夜(前日15:00→当日9:00)が上げなら見送る(15年+82→+99%・XM t=3.2・月勝率58→61%)
input double InpJpPrevNightMax = 0.0;       // 前夜の上げがこの%以下の日だけ建てる
input int    InpJpMaxSpread   = 20;         // 許容スプレッド(pt=1円)
input long   InpJpMagic       = 20260909;

input bool   InpUsOn          = true;       // US500夜ドリフトを動かす(3本目・日経と同型・前夜≤0)
input string InpUsSymbol      = "US500Cash";
input double InpUsJpyPer01    = 40000;      // 0.1lot(名目約11万円)あたりの必要残高(円)
input double InpUsLotMax      = 50.0;
input int    InpUsMaxSpread   = 150;        // 許容スプレッド(pt=0.01$)
input long   InpUsMagic       = 20260913;

input bool   InpDeOn          = true;       // GER40 欧州の夜(01:00JST買→16:00JST売・火〜金JST・直前レッグ≤0)
input string InpDeSymbol      = "GER40Cash";
input double InpDeJpyPer01    = 50000;      // 0.1lot(名目約46万円)あたりの必要残高(円)
input double InpDeLotMax      = 50.0;
input int    InpDeMaxSpread   = 400;        // 許容スプレッド(pt=0.01EUR)
input int    InpDeEntryHour   = 1;          // 買い時刻 JST
input int    InpDeExitHour    = 16;         // 手仕舞い時刻 JST
input long   InpDeMagic       = 20260914;
//--- C) 金 Globex再開買い（サーバー01:05買→03:00売・13年t8.2・14/14年・0.01lot=名目68万円）
input int    InpGxMode        = 1;          // 0=off / 1=紙(仮想約定をログとFilesに記録) / 2=実弾(残高がInpGxMinBalance以上のときだけ)
input double InpGxMinBalance  = 200000;     // 実弾に切り替える残高(円)・未満なら紙のまま
input int    InpGxEntryHourSrv = 1;         // 建て時刻 サーバー(時)・メンテ明け01:05
input int    InpGxEntryMinSrv  = 5;
input int    InpGxExitHourSrv  = 3;         // 決済時刻 サーバー(時)
input int    InpGxMaxSpread   = 25;         // 許容スプレッド(pt=0.01$)・超えたら最大60秒待つ
input double InpGxJpyPer001   = 20000;      // 0.01lotあたりの必要残高(円)
input double InpGxLotMax      = 0.10;
input long   InpGxMagic       = 20260910;

CTrade   trade;
datetime g_goldEntryDay = 0, g_goldEntryTime = 0;
datetime g_jpEntryDay = 0, g_jpExitDay = 0, g_usEntryDay = 0, g_usExitDay = 0, g_deEntryDay = 0, g_deExitDay = 0;
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
double LotGold()
{
   double lot = MathFloor(AccountInfoDouble(ACCOUNT_BALANCE) / InpGoldJpyPer001) * 0.01;
   double vmin = SymbolInfoDouble(InpGoldSymbol, SYMBOL_VOLUME_MIN), vstep = SymbolInfoDouble(InpGoldSymbol, SYMBOL_VOLUME_STEP);
   lot = MathMax(vmin, MathMin(InpGoldLotMax, lot)); return NormalizeDouble(MathFloor(lot / vstep) * vstep, 2);
}
//--- 前夜リターン: 当日9:00JSTのH1始値 ÷ 前日15:00JSTのH1始値 - 1（%）。取れなければ NaN 扱い(建てる)
double PrevNightPct(string sym)
{
   datetime srvOff = TimeTradeServer() - TimeGMT();          // サーバー時刻 - GMT
   datetime jst = NowJST(); datetime dayJ = DayOf(jst);
   datetime t9  = dayJ + 9 * 3600 - 9 * 3600 + srvOff;       // 当日09:00JST をサーバー時刻に
   datetime t15 = dayJ - 86400 + 15 * 3600 - 9 * 3600 + srvOff; // 前日15:00JST
   MqlDateTime dw; TimeToStruct(jst, dw);
   if(dw.day_of_week == 1) t15 -= 3 * 86400;                  // 月曜は金曜15:00
   int b9 = iBarShift(sym, PERIOD_H1, t9, true), b15 = iBarShift(sym, PERIOD_H1, t15, true);
   if(b9 < 0 || b15 < 0) return 0.0;
   double o9 = iOpen(sym, PERIOD_H1, b9), o15 = iOpen(sym, PERIOD_H1, b15);
   if(o9 <= 0 || o15 <= 0) return 0.0;
   return (o9 / o15 - 1.0) * 100.0;
}

double LotIdx(string sym, double jpyPerUnit, double unit, double lotMax)
{
   double vmin = SymbolInfoDouble(sym, SYMBOL_VOLUME_MIN), vstep = SymbolInfoDouble(sym, SYMBOL_VOLUME_STEP);
   if(vstep <= 0) vstep = vmin;
   double lot = MathFloor(AccountInfoDouble(ACCOUNT_BALANCE) / jpyPerUnit * unit / vstep + 1e-9) * vstep;   // 残高÷単位残高×単位lot をvstep刻みで切り捨て
   lot = MathMax(vmin, MathMin(lotMax, lot)); return NormalizeDouble(lot, 2);
}
double LotJp() { return LotIdx(InpJpSymbol, InpJpJpyPerLot, 1.0, InpJpLotMax); }
double LotUs() { return LotIdx(InpUsSymbol, InpUsJpyPer01, 0.1, InpUsLotMax); }
double LotDe() { return LotIdx(InpDeSymbol, InpDeJpyPer01, 0.1, InpDeLotMax); }
//--- 直前レッグ: 直近の exitH 足(now以前)と、その前の entryH 足。間隔が26h超(週末跨ぎ)なら一つ前のexitH足へ戻る。取れなければ0(=建てる)
double PrevLegPct(string sym, int entryH, int exitH)
{
   datetime srvOff = TimeTradeServer() - TimeGMT();
   datetime now = TimeTradeServer();
   for(int back = 0; back < 8; back++)
   {
      datetime dayJ = DayOf(NowJST()) - back * 86400;
      datetime tE = dayJ + exitH * 3600 - 9 * 3600 + srvOff;          // その日のexitH(JST)をサーバー時刻に
      if(tE > now) continue;
      int bE = iBarShift(sym, PERIOD_H1, tE, true);
      if(bE < 0) continue;
      datetime tA = tE - (exitH - entryH) * 3600;                       // 同日 entryH
      if(entryH > exitH) tA -= 86400;
      int bA = iBarShift(sym, PERIOD_H1, tA, true);
      if(bA < 0) continue;
      double oA = iOpen(sym, PERIOD_H1, bA), oE = iOpen(sym, PERIOD_H1, bE);
      if(oA <= 0 || oE <= 0) continue;
      return (oE / oA - 1.0) * 100.0;
   }
   return 0.0;
}
void DeTick()
{
   datetime jst = NowJST(); MqlDateTime dt; TimeToStruct(jst, dt); datetime today = DayOf(jst);
   if(dt.hour >= InpDeExitHour && HasPos(InpDeSymbol, InpDeMagic) && g_deExitDay != today)
   { if(CanTrade()) CloseAll(InpDeSymbol, InpDeMagic, "GER40"); g_deExitDay = today; return; }
   bool okDay = (dt.day_of_week >= 2 && dt.day_of_week <= 5);        // 火〜金JST(=欧州の月〜木の夜)
   if(dt.hour >= InpDeEntryHour && dt.hour < InpDeExitHour && okDay && !HasPos(InpDeSymbol, InpDeMagic) && g_deEntryDay != today)
   {
      if(dt.hour >= InpDeEntryHour + 3) { g_deEntryDay = today; return; }
      if(Halted()) { g_deEntryDay = today; return; }
      int spread = (int)SymbolInfoInteger(InpDeSymbol, SYMBOL_SPREAD);
      if(spread > InpDeMaxSpread) { PrintFormat("[GER40] スプレッド%dpt > %d 見送り(再試行)", spread, InpDeMaxSpread); return; }
      double pn = PrevLegPct(InpDeSymbol, InpDeEntryHour, InpDeExitHour);
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
   if(InpGoldOn && !SymbolSelect(InpGoldSymbol, true)) { Print("銘柄が見つからない: ", InpGoldSymbol); return INIT_FAILED; }
   if(InpJpOn && !SymbolSelect(InpJpSymbol, true)) { Print("銘柄が見つからない: ", InpJpSymbol); return INIT_FAILED; }
   if(InpUsOn && !SymbolSelect(InpUsSymbol, true)) { Print("銘柄が見つからない: ", InpUsSymbol); return INIT_FAILED; }
   if(InpDeOn && !SymbolSelect(InpDeSymbol, true)) { Print("銘柄が見つからない: ", InpDeSymbol); return INIT_FAILED; }
   if(!CanTrade()) Print("⚠ デモ口座ではないので発注しません(InpDemoOnly=true)");
   PrintFormat("XMCombo 起動: 残高%.0f円 全停止ライン%.0f円 | 金再開買い mode=%d(2=実弾は残高%.0f以上) | 金%s lot=%.2f(%.0f円ごと0.01・上限%.2f) SL$%.1f 売London%02d:%02d→%d分 | 日経%s lot=%.1f(%.0f円ごと1.0・上限%.1f) 買%02d:00JST→売%02d:00 週末%s 前夜フィルタ%s | US500%s lot=%.1f(%.0f円ごと0.1・上限%.1f) | GER40%s lot=%.1f(%.0f円ごと0.1・上限%.1f) 買%02d:00JST→売%02d:00 火〜金 直前レッグ≤0 | UK-DST=%s",
               AccountInfoDouble(ACCOUNT_BALANCE), InpStopBelowBalance, InpGxMode, InpGxMinBalance, InpGoldOn ? "on" : "off", LotGold(), InpGoldJpyPer001, InpGoldLotMax, InpGoldStopUsd, InpGoldHourLon, InpGoldMinLon, InpGoldHoldMin,
               InpJpOn ? "on" : "off", LotJp(), InpJpJpyPerLot, InpJpLotMax, InpJpEntryHour, InpJpExitHour, InpJpHoldWeekend ? "on" : "off", InpJpPrevNightFilter ? "on" : "off", InpUsOn ? "on" : "off", LotUs(), InpUsJpyPer01, InpUsLotMax, InpDeOn ? "on" : "off", LotDe(), InpDeJpyPer01, InpDeLotMax, InpDeEntryHour, InpDeExitHour, UkDst(TimeGMT()) ? "夏" : "冬");
   EventSetTimer(5);
   return INIT_SUCCEEDED;
}
void OnDeinit(const int reason) { EventKillTimer(); }

void GoldTick()
{
   datetime lon = NowLondon(); MqlDateTime dt; TimeToStruct(lon, dt); datetime today = DayOf(lon);
   int nowMin = dt.hour * 60 + dt.min, entMin = InpGoldHourLon * 60 + InpGoldMinLon;
   if(HasPos(InpGoldSymbol, InpGoldMagic) && g_goldEntryTime > 0 && lon >= g_goldEntryTime + InpGoldHoldMin * 60)
   { if(CanTrade()) CloseAll(InpGoldSymbol, InpGoldMagic, "金"); g_goldEntryTime = 0; return; }
   if(dt.day_of_week >= 1 && dt.day_of_week <= 5 && nowMin >= entMin && nowMin < entMin + 2 && !HasPos(InpGoldSymbol, InpGoldMagic) && g_goldEntryDay != today)
   {
      if(Halted()) { g_goldEntryDay = today; return; }
      int spread = (int)SymbolInfoInteger(InpGoldSymbol, SYMBOL_SPREAD);
      g_goldEntryDay = today;
      if(spread > InpGoldMaxSpread) { PrintFormat("[金] スプレッド%dpt > %d 見送り", spread, InpGoldMaxSpread); return; }
      double lots = LotGold();
      if(!CanTrade()) { PrintFormat("[金][デモ以外] 売りシグナル lot=%.2f（発注せず）", lots); return; }
      double slpx = InpGoldStopUsd > 0 ? NormalizeDouble(SymbolInfoDouble(InpGoldSymbol, SYMBOL_ASK) + InpGoldStopUsd, (int)SymbolInfoInteger(InpGoldSymbol, SYMBOL_DIGITS)) : 0.0;
      trade.SetExpertMagicNumber(InpGoldMagic);
      if(trade.Sell(lots, InpGoldSymbol, 0, slpx, 0, "fix"))
      { g_goldEntryTime = lon; PrintFormat("[金] 売り lot=%.2f(残高%.0f円) @%.2f SL=%.2f spread=%dpt London %02d:%02d", lots, AccountInfoDouble(ACCOUNT_BALANCE), trade.ResultPrice(), slpx, spread, dt.hour, dt.min); }
      else PrintFormat("[金] 売り失敗 ret=%d %s", trade.ResultRetcode(), trade.ResultRetcodeDescription());
   }
}

void IdxTick(string sym, long magic, double lots, int maxSpread, string tag, datetime &entryDay, datetime &exitDay)
{
   datetime jst = NowJST(); MqlDateTime dt; TimeToStruct(jst, dt); datetime today = DayOf(jst);
   if(dt.hour >= InpJpExitHour && dt.hour < InpJpEntryHour && HasPos(sym, magic) && exitDay != today)
   { if(CanTrade()) CloseAll(sym, magic, tag); exitDay = today; return; }
   bool okDay = (dt.day_of_week >= 1 && dt.day_of_week <= 4) || (InpJpHoldWeekend && dt.day_of_week == 5);
   if(dt.hour >= InpJpEntryHour && okDay && !HasPos(sym, magic) && entryDay != today)
   {
      if(dt.hour >= InpJpEntryHour + 3) { entryDay = today; return; }
      if(Halted()) { entryDay = today; return; }
      int spread = (int)SymbolInfoInteger(sym, SYMBOL_SPREAD);
      if(spread > maxSpread) { PrintFormat("[%s] スプレッド%dpt > %d 見送り(再試行)", tag, spread, maxSpread); return; }
      if(InpJpPrevNightFilter)
      {
         double pn = PrevNightPct(sym);
         if(pn > InpJpPrevNightMax) { PrintFormat("[%s] 前夜%+.2f%% > %.2f%% なので今夜は見送り", tag, pn, InpJpPrevNightMax); entryDay = today; return; }
         PrintFormat("[%s] 前夜%+.2f%% → 建てる", tag, pn);
      }
      if(!CanTrade()) { PrintFormat("[%s][デモ以外] 買いシグナル lot=%.1f（発注せず）", tag, lots); entryDay = today; return; }
      trade.SetExpertMagicNumber(magic);
      if(trade.Buy(lots, sym, 0, 0, 0, "night")) PrintFormat("[%s] 買い lot=%.1f(残高%.0f円) @%.2f spread=%dpt", tag, lots, AccountInfoDouble(ACCOUNT_BALANCE), trade.ResultPrice(), spread);
      else PrintFormat("[%s] 買い失敗 ret=%d %s", tag, trade.ResultRetcode(), trade.ResultRetcodeDescription());
      entryDay = today;
   }
}
void JpTick() { IdxTick(InpJpSymbol, InpJpMagic, LotJp(), InpJpMaxSpread, "日経", g_jpEntryDay, g_jpExitDay); }
void UsTick() { IdxTick(InpUsSymbol, InpUsMagic, LotUs(), InpUsMaxSpread, "US500", g_usEntryDay, g_usExitDay); }

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

void OnTimer()
{
   GxTick();
   if(InpGoldOn) GoldTick();
   if(InpJpOn) JpTick();
   if(InpUsOn) UsTick();
   if(InpDeOn) DeTick();
}
//+------------------------------------------------------------------+
