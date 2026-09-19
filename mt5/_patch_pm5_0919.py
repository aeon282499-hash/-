# -*- coding: utf-8 -*-
"""9/19 XMCombo に PM値決め直前5分ショートを追加するパッチ(AM/PMを CFixLeg クラスで独立管理)。一度だけ実行。"""
p='mt5/XMCombo.mq5'; s=open(p,encoding='utf-8-sig').read(); orig=s
# 1) inputs
s=s.replace('input double InpGoldGateThr   = 0.10;       // 閾値 $/oz\n',
'''input double InpGoldGateThr   = 0.10;       // 閾値 $/oz
input bool   InpGoldPmOn      = true;       // PM値決め(ロンドン15:00)直前5分ショート(9/19採用: 10年%t5.5・10/11年+・XM M5 17ヶ月t3.7・現行に足してE[log]5.09→5.66・DD不変・同サイズ/同ゲート/同SL)
input int    InpGoldPmHourLon = 14;         // 売り時刻 ロンドン(時)
input int    InpGoldPmMinLon  = 55;         // 売り時刻 ロンドン(分)
input int    InpGoldPmHoldMin = 5;          // 保有分数(15分窓はnet-0.01で不可・5分だけ生きる)
input long   InpGoldPmMagic   = 20260919;
''',1)
# 2) globals
s=s.replace('datetime g_goldEntryDay = 0, g_goldEntryTime = 0; double g_gfHist[]; datetime g_gfHistDay[]; datetime g_gfRecDay = 0; bool g_gfInit = false;\n','',1)
# 3) warm/select
s=s.replace('if(InpGoldOn || InpGdOn) { iTime(InpGoldSymbol, PERIOD_H1, 1); iTime(InpGoldSymbol, PERIOD_M1, 1); }','if(InpGoldOn || InpGoldPmOn || InpGdOn) { iTime(InpGoldSymbol, PERIOD_H1, 1); iTime(InpGoldSymbol, PERIOD_M1, 1); }',1)
s=s.replace('if(InpGoldOn && !SymbolSelect(InpGoldSymbol, true))','if((InpGoldOn || InpGoldPmOn || InpGdOn) && !SymbolSelect(InpGoldSymbol, true))',1)
# 4) replace gold fix block
a=s.index('//--- 金15分窓の紙の結果'); b=s.index('\nvoid IdxTick(')
cls=r'''//--- 値決め直前ショートのレッグ。AM(10:15→10:30)とPM(14:55→15:00)をロンドン時刻で同じ仕組みで動かす。ゲート履歴・建玉状態はレッグごとに独立
//    窓の紙の結果($/oz・SL・コスト$0.21): London day の 開始始値 - 終了始値
class CFixLeg
{
public:
   string   tag; int hourLon, minLon, holdMin; long magic;
   double   hist[]; datetime histDay[]; datetime recDay; bool init; int initTries;
   datetime entryDay, entryTime;
   void Setup(string t, int h, int m, int hold, long mg)
   { tag = t; hourLon = h; minLon = m; holdMin = hold; magic = mg; recDay = 0; init = false; initTries = 0; entryDay = 0; entryTime = 0; ArrayResize(hist, 0); ArrayResize(histDay, 0); }
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
      res = (InpGoldStopUsd > 0 && hi >= o1 + InpGoldStopUsd) ? -InpGoldStopUsd - 0.21 : (o1 - o2) - 0.21;
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
         double r; if(Window(day, r)) { GateAppend(day, r); got++; }
      }
      int n = ArraySize(hist);   // 古い→新しい順
      for(int i = 0; i < n / 2; i++) { double t = hist[i]; hist[i] = hist[n - 1 - i]; hist[n - 1 - i] = t; datetime td = histDay[i]; histDay[i] = histDay[n - 1 - i]; histDay[n - 1 - i] = td; }
      initTries++;
      if(n < InpGoldGateN && initTries < 60) return;          // M1履歴の読込待ち(5秒×60)
      init = true;
      PrintFormat("[%sゲート] 履歴%d本を復元 直近%d回平均=%+.3f$/oz (閾値%.2f・%s)", tag, n, InpGoldGateN, GateMean(), InpGoldGateThr, GateOpen() ? "稼働" : "休止");
   }
   double GateMean()
   {
      int n = ArraySize(hist); if(n == 0) return 0; int k = MathMin(n, InpGoldGateN); double s = 0;
      for(int i = n - k; i < n; i++) s += hist[i]; return s / k;
   }
   bool GateOpen() { if(InpGoldGateN <= 0) return true; if(ArraySize(hist) < InpGoldGateN) return true; return GateMean() > InpGoldGateThr; }
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
         double rb; if(Window(day, rb)) GateAppend(day, rb);
      }
      double r; if(!Window(today, r)) return;
      GateAppend(today, r); recDay = today;
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
         if(!GateOpen()) { PrintFormat("[%s] ゲート休止: 直近%d回平均%+.3f$/oz ≤ %.2f → 撃たない(紙で計測は継続)", tag, InpGoldGateN, GateMean(), InpGoldGateThr); entryDay = today; return; }
         int spread = (int)SymbolInfoInteger(InpGoldSymbol, SYMBOL_SPREAD);
         entryDay = today;
         if(spread > InpGoldMaxSpread) { PrintFormat("[%s] スプレッド%dpt > %d 見送り", tag, spread, InpGoldMaxSpread); return; }
         double lots = LotGold();
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
'''
s=s[:a]+cls+s[b:]
# 5) OnTimer
s=s.replace('   if(InpGoldOn) { GoldGateInit(); GoldGateRecord(); }\n','   if(InpGoldOn)   { g_fixAM.GateInit(); g_fixAM.GateRecord(); }\n   if(InpGoldPmOn) { g_fixPM.GateInit(); g_fixPM.GateRecord(); }\n',1)
s=s.replace('   if(InpGoldOn) GoldTick();\n','   if(InpGoldOn) g_fixAM.Tick();\n   if(InpGoldPmOn) g_fixPM.Tick();\n',1)
# 6) OnInit: setup + print
s=s.replace('   trade.SetDeviationInPoints(30);\n','   trade.SetDeviationInPoints(30);\n   g_fixAM.Setup("金", InpGoldHourLon, InpGoldMinLon, InpGoldHoldMin, InpGoldMagic);\n   g_fixPM.Setup("金PM", InpGoldPmHourLon, InpGoldPmMinLon, InpGoldPmHoldMin, InpGoldPmMagic);\n',1)
s=s.replace('   PrintFormat("[ラダー設定]','   PrintFormat("[金PM] %s 売London%02d:%02d→%d分 lot=%.2f(AMと同サイズ/同ゲートN%d閾%.2f/同SL) magic=%I64d", InpGoldPmOn ? "on" : "off", InpGoldPmHourLon, InpGoldPmMinLon, InpGoldPmHoldMin, LotGold(), InpGoldGateN, InpGoldGateThr, InpGoldPmMagic);\n   PrintFormat("[ラダー設定]',1)
assert s!=orig and 'GoldTick' not in s and 'g_gfHist' not in s and 'GoldGateInit' not in s, 'patch incomplete'
open(p,'w',encoding='utf-8-sig').write(s); print('patched lines',s.count('\n'))
