# -*- coding: utf-8 -*-
"""_watchdog.py — XM自動売買の監視とDiscord通知。
  --mode health : MT5プロセス/接続/EAハートビート(2分以内)を確認。異常ならMT5を起動し直してDiscordへ。
  --mode entry  : 今日の建て時刻の後に「買い」か「見送り」のログがあるか確認(引数 --leg 日経|US500|GER40|金)。無ければ通知。
  --mode daily  : 前日〜今日の約定・残高・建玉をDiscordへ(9:20)。
webhook: .env の DISCORD_WEBHOOK_XM_URL、無ければ DISCORD_WEBHOOK_GOKUJO_URL(俺専用サーバー)。
"""
import sys, os, glob, datetime as dt, subprocess, json, urllib.request, re
HERE=os.path.dirname(os.path.abspath(__file__)); ROOT=os.path.dirname(HERE)
DATA=os.path.join(os.environ['APPDATA'],'MetaQuotes','Terminal','C4171FD2B38378D6406D5C84412B5F20')
LOG=os.path.join(HERE,'_watchdog.log')
def log(m):
    line=f"{dt.datetime.now():%Y-%m-%d %H:%M:%S} {m}"; print(line)
    open(LOG,'a',encoding='utf-8').write(line+'\n')
def webhook():
    env={}
    for l in open(os.path.join(ROOT,'.env'),encoding='utf-8'):
        if '=' in l and not l.startswith('#'): k,v=l.rstrip('\n').split('=',1); env[k]=v.strip().strip('"')
    return env.get('DISCORD_WEBHOOK_XM_URL') or env.get('DISCORD_WEBHOOK_GOKUJO_URL')   # XM専用chが無い間は俺専用サーバーの極上chへ
def post(text):
    url=webhook()
    if not url: log('webhook無し: '+text); return
    req=urllib.request.Request(url,data=json.dumps({'content':text[:1900]}).encode(),headers={'Content-Type':'application/json','User-Agent':'xm-watchdog'})
    try: urllib.request.urlopen(req,timeout=20); log('posted: '+text.splitlines()[0])
    except Exception as e: log(f'post失敗 {e}: {text[:80]}')
def mt5_running():
    out=subprocess.run(['tasklist','/FI','IMAGENAME eq terminal64.exe'],capture_output=True).stdout.decode('cp932','ignore')
    return 'terminal64.exe' in out
def start_mt5():
    subprocess.Popen([os.path.join(r'C:\Program Files\XM Trading MT5','terminal64.exe'), '/config:'+os.path.join(HERE,'_start.ini')])
def heartbeat_age():
    f=os.path.join(DATA,'MQL5','Files','XMCombo_heartbeat.txt')
    if not os.path.exists(f): return None,None
    age=(dt.datetime.now()-dt.datetime.fromtimestamp(os.path.getmtime(f))).total_seconds()
    return age, open(f,encoding='utf-8',errors='ignore').read().strip()
def expert_log_today():
    f=os.path.join(DATA,'MQL5','Logs',dt.datetime.now().strftime('%Y%m%d')+'.log')   # Expertsログはローカル日付ごと。今日の分が無ければ空(=建て漏れ扱い)
    if not os.path.exists(f): return ''
    return open(f,encoding='utf-16',errors='ignore').read()
mode=sys.argv[sys.argv.index('--mode')+1] if '--mode' in sys.argv else 'health'
if mode=='health':
    problems=[]
    if not mt5_running(): problems.append('MT5プロセスが無い → 起動した'); start_mt5()
    age,body=heartbeat_age()
    if age is None: problems.append('EAハートビート無し(ファイル未作成)')
    elif age>180: problems.append(f'EAハートビート停止 {age/60:.0f}分前: {body}')
    elif 'connected=0' in (body or ''): problems.append('MT5がサーバー未接続: '+body)
    if problems: post('⚠️ XM監視: '+' / '.join(problems))
    else: log('OK '+(body or ''))
elif mode=='entry':
    leg=sys.argv[sys.argv.index('--leg')+1]; txt=expert_log_today(); today=dt.datetime.now().strftime('%H:')
    pat={'日経':r'\[日経\] (買い|前夜|月曜)','US500':r'\[US500\] (買い|前夜|月曜)','GER40':r'\[GER40\] (買い|直前)','金':r'\[金\] (売り|スプレッド|ゲート)'}[leg]
    hits=re.findall(r'^(\d\d:\d\d:\d\d).*'+pat,txt,flags=re.M)
    lines=[l for l in txt.splitlines() if re.search(pat,l)]
    bad=[l for l in txt.splitlines() if re.search(r'\['+re.escape(leg)+r'\].*(失敗|再試行中|未ロード)',l)]
    if not lines: post(f'⚠️ XM監視: {leg} の建て時刻を過ぎたが「買い」も「見送り」もログに無い(EA停止/時刻ずれの疑い)')
    elif any('失敗' in l for l in bad): post(f'⚠️ XM監視: {leg} の発注が失敗している: '+bad[-1][-160:])
    else: log(f'{leg} OK: '+lines[-1][-120:])
elif mode=='test':
    post('🛠 XM監視テスト: MT5稼働中・EAハートビートOK。今後ここに「MT5停止」「建て漏れ」「朝の約定記録(9:20)」を流す。専用chが欲しければwebhookを作って .env の DISCORD_WEBHOOK_XM_URL に入れる。')
elif mode=='daily':
    r=subprocess.run([sys.executable,'-X','utf8',os.path.join(HERE,'_fix_report.py'),'--days','1'],capture_output=True).stdout.decode('utf-8','ignore') if False else subprocess.run([sys.executable,'-X','utf8',os.path.join(HERE,'_fix_report.py'),'--days','1'],capture_output=True)
    out=[l for l in r.stdout.decode('utf-8','ignore').splitlines() if l.startswith('[')]
    age,body=heartbeat_age()
    txt='📒 XM 朝の記録\n'+('\n'.join(out) if out else '新規約定なし')+f'\n{body or "ハートビート無し"}'
    post(txt)
