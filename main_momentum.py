"""
main_momentum.py — 立花型出来高理論シグナル日次配信

実行: python main_momentum.py
推奨: 15:30以降 (大引け後) の日次cron
"""
from screener_volume_theory import run_screener
from notifier_momentum import send_signals


def main():
    target_date, theme_sigs, pure_sigs, ranked, hot = run_screener()
    print(f"\n[main] {target_date}: 🔥追い風 {len(theme_sigs)}件 / "
          f"⚡純モメンタム {len(pure_sigs)}件 / ホットテーマ {len(hot)}件")
    send_signals(theme_sigs, pure_sigs, target_date,
                 position_budget=200_000, ranked=ranked, hot=hot)
    print("[main] Discord送信完了")


if __name__ == "__main__":
    main()
